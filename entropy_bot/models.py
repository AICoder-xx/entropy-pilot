from __future__ import annotations

import math
import time
from dataclasses import asdict, dataclass, field
from typing import Dict, Optional


@dataclass
class OrderBook:
    bids: Dict[float, float] = field(default_factory=dict)
    asks: Dict[float, float] = field(default_factory=dict)
    ready: bool = False
    last_update_ts: float = 0.0
    alive_ts: float = 0.0

    def clear(self) -> None:
        self.bids.clear()
        self.asks.clear()
        self.ready = False

    def touch(self) -> None:
        self.alive_ts = time.time()

    def apply_hl(self, levels: list) -> None:
        self.bids = {
            float(x["px"]): float(x["sz"])
            for x in (levels[0] if levels else [])
            if float(x.get("sz") or 0) > 0
        }
        self.asks = {
            float(x["px"]): float(x["sz"])
            for x in (levels[1] if len(levels) > 1 else [])
            if float(x.get("sz") or 0) > 0
        }
        self.ready = bool(self.bids and self.asks)
        self.last_update_ts = time.time()
        self.touch()

    @property
    def best_bid(self) -> Optional[float]:
        return max(self.bids) if self.bids else None

    @property
    def best_ask(self) -> Optional[float]:
        return min(self.asks) if self.asks else None

    @property
    def mid(self) -> Optional[float]:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / 2.0

    @property
    def microprice(self) -> Optional[float]:
        """Queue-imbalance weighted mid (Stoikov's first-level microprice).

        A heavy bid queue pushes fair value toward the ask and vice versa;
        it leads the plain mid by roughly one tick of information, which is
        exactly the edge a passive quoter needs to stop buying into
        imminent down-moves.
        """
        bid, ask = self.best_bid, self.best_ask
        if bid is None or ask is None:
            return None
        bid_sz = self.bids.get(bid, 0.0)
        ask_sz = self.asks.get(ask, 0.0)
        total = bid_sz + ask_sz
        if total <= 0:
            return (bid + ask) / 2.0
        return (bid_sz * ask + ask_sz * bid) / total

    @property
    def spread_bps(self) -> Optional[float]:
        if self.best_bid is None or self.best_ask is None or self.best_bid <= 0:
            return None
        return (self.best_ask / self.best_bid - 1.0) * 10_000

    def is_fresh(self, max_age_sec: float) -> bool:
        return self.ready and time.time() - self.alive_ts <= max_age_sec


@dataclass
class MarketMeta:
    coin: str
    dex: str
    symbol: str
    sz_decimals: int
    max_leverage: int
    only_isolated: bool
    growth_mode: bool
    deployer_fee_scale: float
    is_delisted: bool
    dex_index: int
    universe_index: int
    asset_id: int

    @property
    def size_step(self) -> float:
        return 10 ** -self.sz_decimals

    def px_round(self, px: float, round_up: bool) -> float:
        """Hyperliquid price rule: at most 5 significant figures and
        MAX_DECIMALS - szDecimals decimal places for perps."""
        if px <= 0:
            return px
        max_dec = max(0, 6 - self.sz_decimals)
        sig_dec = 4 - math.floor(math.log10(px))
        dec = max(0, min(max_dec, sig_dec))
        factor = 10.0**dec
        value = (
            math.ceil(px * factor - 1e-9) / factor
            if round_up
            else math.floor(px * factor + 1e-9) / factor
        )
        return round(value, 8)

    def qty_floor(self, qty: float) -> float:
        step = self.size_step
        return round(math.floor(qty / step + 1e-9) * step, self.sz_decimals)


@dataclass
class MarketSnapshot:
    meta: MarketMeta
    book: OrderBook
    mark_px: Optional[float] = None
    oracle_px: Optional[float] = None
    funding: Optional[float] = None
    open_interest: Optional[float] = None
    day_notional_volume: Optional[float] = None
    context_ts: float = 0.0

    @property
    def fair_px(self) -> Optional[float]:
        values = [
            x
            for x in (self.book.mid, self.mark_px, self.oracle_px)
            if x is not None and x > 0
        ]
        if not values:
            return None
        values.sort()
        return values[len(values) // 2]


@dataclass
class FeeModel:
    user_maker_rate: float
    user_taker_rate: float
    active_hl_referral_discount: float
    deployer_fee_scale: float
    deployer_share: float
    growth_mode: bool
    gross_maker_rate: float
    gross_taker_rate: float
    entropy_self_rebate_pct: float
    entropy_referred_benefit_pct: float

    def projected_entropy_rebate(self, actual_fee_usd: float) -> tuple[float, float]:
        if actual_fee_usd <= 0:
            return 0.0, 0.0
        share = actual_fee_usd * self.deployer_share
        own = share * self.entropy_self_rebate_pct / 100.0
        referred = share * self.entropy_referred_benefit_pct / 100.0
        return own, referred

    @property
    def projected_net_maker_rate(self) -> float:
        if self.gross_maker_rate <= 0:
            return self.gross_maker_rate
        rebate_mult = self.deployer_share * (
            self.entropy_self_rebate_pct + self.entropy_referred_benefit_pct
        ) / 100.0
        return self.gross_maker_rate * (1.0 - rebate_mult)

    @property
    def projected_net_taker_rate(self) -> float:
        if self.gross_taker_rate <= 0:
            return self.gross_taker_rate
        rebate_mult = self.deployer_share * (
            self.entropy_self_rebate_pct + self.entropy_referred_benefit_pct
        ) / 100.0
        return self.gross_taker_rate * (1.0 - rebate_mult)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["gross_maker_bps"] = self.gross_maker_rate * 10_000
        d["gross_taker_bps"] = self.gross_taker_rate * 10_000
        d["projected_net_maker_bps"] = self.projected_net_maker_rate * 10_000
        d["projected_net_taker_bps"] = self.projected_net_taker_rate * 10_000
        return d


@dataclass
class OrderIntent:
    role: str
    is_buy: bool
    price: float
    qty: float
    reduce_only: bool = False
    tif: str = "Alo"

    @property
    def notional(self) -> float:
        return self.price * self.qty


@dataclass
class TrackedOrder:
    role: str
    cloid: str
    is_buy: bool
    price: float
    qty: float
    reduce_only: bool
    tif: str
    created_ts: float
    oid: Optional[int] = None
    filled_qty: float = 0.0

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class Fill:
    fill_id: str
    order_role: str
    cloid: str
    oid: Optional[int]
    is_buy: bool
    price: float
    qty: float
    fee_usd: float
    liquidity: str
    time_ms: int
    realized_pnl_delta: float = 0.0
    projected_self_rebate: float = 0.0
    projected_referred_benefit: float = 0.0
    # Paper fills know their USD fee; live ingestion overrides every flag
    # from the official row rather than silently treating missing data as 0.
    start_position_qty: Optional[float] = None
    fee_token: Optional[str] = "USDC"
    fee_known: bool = True
    realized_pnl_known: bool = True

    @property
    def notional(self) -> float:
        return self.price * self.qty


@dataclass
class AccountState:
    position_qty: float = 0.0
    entry_px: float = 0.0
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    equity_actual: float = 0.0
    withdrawable: Optional[float] = None
    liquidation_px: Optional[float] = None
    account_abstraction: str = "default"
    collateral_source: str = "io_dex"
    available_to_buy_usd: Optional[float] = None
    available_to_sell_usd: Optional[float] = None
    fees_paid: float = 0.0
    projected_self_rebate: float = 0.0
    projected_referred_benefit: float = 0.0
    volume_usd: float = 0.0
    fee_accounting_complete: bool = True
    realized_accounting_complete: bool = True

    @property
    def projected_rebate_total(self) -> float:
        return self.projected_self_rebate + self.projected_referred_benefit

    @property
    def equity_after_projected_rebate(self) -> float:
        return self.equity_actual + self.projected_rebate_total
