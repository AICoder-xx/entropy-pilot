from __future__ import annotations

import logging
import math
import time
from abc import ABC, abstractmethod
from collections import deque

from .config import Config
from .models import AccountState, FeeModel, Fill, MarketSnapshot, OrderIntent

log = logging.getLogger("entropy.strategy")

VOLATILITY_GUARD_WINDOW_SEC = 60.0
# Inventory stuck above the rebalance threshold for this long escalates the
# passive exit from "queue at own side" to "front of the spread" (still ALO
# maker: it improves the book by one tick, so any counter-flow hits us first).
INVENTORY_IMPROVE_AFTER_SEC = 120.0
# Realized-volatility estimator (EWMA of squared log mid-returns, normalized
# per second) feeding the Avellaneda-Stoikov style quote scaling.  The
# horizon matches the 30-60 s holding period the wear ledger measures.
SIGMA_HALFLIFE_SEC = 60.0
SIGMA_HORIZON_SEC = 60.0


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _allocate_intents(
    intents: list[OrderIntent],
    *,
    account: AccountState,
    fair: float,
    max_position_usd: float,
    max_open_order_usd: float,
    min_notional: float,
    qty_floor,
    entry_limit_usd: float | None = None,
) -> list[OrderIntent]:
    """Clamp each side against worst-case filled exposure, not current exposure.
    This prevents several valid-looking quotes from jointly exceeding the cap.
    """
    if not math.isfinite(fair) or fair <= 0:
        return []
    operational_limit = (
        max_position_usd if entry_limit_usd is None else min(entry_limit_usd, max_position_usd)
    )
    # The risk cap is marked at current fair, not each quote's cheaper limit.
    # Otherwise several low bids collectively buy more inventory than the cap.
    buy_room = max(operational_limit / fair - account.position_qty, 0.0)
    sell_room = max(operational_limit / fair + account.position_qty, 0.0)
    open_room = max_open_order_usd
    result: list[OrderIntent] = []
    for intent in intents:
        side_room = buy_room if intent.is_buy else sell_room
        qty = qty_floor(min(intent.qty, side_room, open_room / intent.price)) if intent.price > 0 else 0.0
        notional = qty * intent.price
        if qty <= 0 or notional + 1e-9 < min_notional:
            continue
        result.append(
            OrderIntent(
                role=intent.role,
                is_buy=intent.is_buy,
                price=intent.price,
                qty=qty,
                reduce_only=intent.reduce_only,
                tif=intent.tif,
            )
        )
        if intent.is_buy:
            buy_room -= qty
        else:
            sell_room -= qty
        open_room -= notional
    return result


class Strategy(ABC):
    name: str

    def __init__(self, cfg: Config, fees: FeeModel) -> None:
        self.cfg = cfg
        self.fees = fees

    @abstractmethod
    def desired(self, market: MarketSnapshot, account: AccountState) -> list[OrderIntent]: ...

    def on_fill(self, fill: Fill) -> None:
        del fill

    def on_markout(self, markout: dict) -> bool:
        del markout
        return False

    def halt_reason(self, market: MarketSnapshot, account: AccountState) -> str | None:
        del market, account
        return None

    def state_dict(self) -> dict:
        return {}

    def load_state(self, state: dict) -> None:
        del state


class MarketMakerStrategy(Strategy):
    name = "market_maker"

    def __init__(self, cfg: Config, fees: FeeModel) -> None:
        super().__init__(cfg, fees)
        window = cfg.market_maker.adaptive_spread_window_samples
        self._adaptive_markouts: deque[tuple[float, float, float]] = deque(
            maxlen=window
        )
        self._adaptive_extra_bps = 0.0
        self._adaptive_samples_since_evaluation = 0
        self._adaptive_last_observation_wall = 0.0
        self._adaptive_last_decay_wall = 0.0
        # Shock brake: toxic sweep bursts can outrun regular evaluations.
        # Watch a short rolling window and jump to the configured maximum
        # extra spread when execution wear greatly exceeds the trigger.
        # Thresholds derive from existing configuration settings.
        shock_window = max(cfg.market_maker.adaptive_spread_min_samples // 5, 10)
        self._shock_markouts: deque[tuple[float, float]] = deque(
            maxlen=shock_window
        )
        # Volatility guard: rolling 60 s mid history.  When the realized range
        # exceeds the configured threshold, quoting becomes exposure-reducing
        # only (both sides pulled at flat position) until the range halves.
        self._mid_history: deque[tuple[float, float]] = deque()
        self._volatility_guard_active = False
        self._last_volatility_range_bps = 0.0
        # Same-side fill cooldown uses the monotonic timestamp of the latest
        # fill per side (True=buy). Delaying exposure-increasing re-quotes
        # avoids feeding another order into the same directional sweep.
        self._last_fill_at: dict[bool, float] = {True: 0.0, False: 0.0}
        # Crisis (dislocation) mode: set by the engine when the book is too
        # wide/empty for normal quoting but the oracle anchor is alive.
        self.crisis_mode = False
        # Inventory aging tracks when the position first crosses the rebalance
        # threshold without returning. A trapped position may not fill at its
        # own side of the book; after INVENTORY_IMPROVE_AFTER_SEC, the exit
        # order moves inside the spread.
        self._rebalance_breach_since: float | None = None
        # EWMA realized variance of the mid (bps^2 per second).
        self._sigma_var_bps2 = 0.0
        self._sigma_last: tuple[float, float] | None = None
        self._last_quote_metrics: dict[str, float | bool | int | str | None] = {}

    @property
    def adaptive_extra_bps(self) -> float:
        return self._adaptive_extra_bps

    @property
    def adaptive_metrics(self) -> dict:
        cfg = self.cfg.market_maker
        now_wall = time.time()
        self._prune_adaptive_samples(now_wall)
        observed = sum(item[0] for item in self._adaptive_markouts)
        markout_pnl = sum(item[1] for item in self._adaptive_markouts)
        weighted_markout_bps = (
            markout_pnl / observed * 10_000 if observed > 0 else None
        )
        projected_wear_bps = (
            self.fees.projected_net_maker_rate * 10_000 - weighted_markout_bps
            if weighted_markout_bps is not None
            else None
        )
        return {
            "window_samples": len(self._adaptive_markouts),
            "minimum_samples": cfg.adaptive_spread_min_samples,
            "samples_since_evaluation": self._adaptive_samples_since_evaluation,
            "samples_until_evaluation": max(
                cfg.adaptive_spread_adjust_every_samples
                - self._adaptive_samples_since_evaluation,
                0,
            ),
            "observed_notional_usd": observed,
            "weighted_markout_bps": weighted_markout_bps,
            "projected_execution_wear_bps": projected_wear_bps,
            "wear_trigger_bps": cfg.adaptive_spread_wear_trigger_bps,
            "extra_bps": self._adaptive_extra_bps,
            "volatility_range_bps": self._last_volatility_range_bps,
            "volatility_guard_active": self._volatility_guard_active,
            "volatility_guard_range_bps": cfg.volatility_guard_range_bps,
            "crisis_mode_active": self.crisis_mode,
            "sigma_horizon_bps": self.sigma_horizon_bps,
            "regime_key": self._adaptive_regime_key(),
            "last_observation_age_sec": (
                max(now_wall - self._adaptive_last_observation_wall, 0.0)
                if self._adaptive_last_observation_wall > 0
                else None
            ),
            "idle_decay_sec": cfg.adaptive_spread_idle_decay_sec,
            "sample_max_age_sec": cfg.adaptive_spread_sample_max_age_sec,
            **self._last_quote_metrics,
        }

    def _adaptive_regime_key(self) -> str:
        """Identify quote rules whose markouts are statistically comparable."""

        cfg = self.cfg.market_maker
        # Include every reviewed knob that changes quote price, participation,
        # replacement selection or the adaptive controller itself.  Otherwise
        # a one-parameter A/B (for example cooldown or refresh hysteresis)
        # would silently inherit the previous regime's 200 markouts/extra.
        fields = (
            ("adaptive_enabled", int(cfg.adaptive_spread_enabled)),
            ("levels", cfg.levels),
            ("side_usd", cfg.order_notional_usd),
            ("level_decay", cfg.level_notional_decay),
            ("half", cfg.half_spread_bps),
            ("spacing", cfg.level_spacing_bps),
            ("min_edge", cfg.min_edge_after_projected_rebate_bps),
            ("inventory_skew", cfg.inventory_skew_bps_at_limit),
            ("rebalance", cfg.rebalance_at_position_ratio),
            ("refresh_bps", cfg.refresh_bps),
            ("refresh_sec", cfg.refresh_sec),
            ("vol_guard", cfg.volatility_guard_range_bps),
            ("fill_cooldown", cfg.same_side_fill_cooldown_sec),
            ("crisis_usd", cfg.crisis_quote_notional_usd),
            ("crisis_bps", cfg.crisis_quote_distance_bps),
            ("vol_spread", cfg.vol_spread_mult),
            ("vol_skew", cfg.vol_skew_mult),
            ("micro", cfg.microprice_weight),
            ("aggr_mult", cfg.aggressive_requote_multiplier),
            ("aggr_age", cfg.aggressive_requote_min_age_sec),
            ("qty_refresh", cfg.qty_refresh_ratio),
            ("adaptive_min", cfg.adaptive_spread_min_samples),
            ("adaptive_window", cfg.adaptive_spread_window_samples),
            ("adaptive_every", cfg.adaptive_spread_adjust_every_samples),
            ("adaptive_trigger", cfg.adaptive_spread_wear_trigger_bps),
            ("adaptive_step", cfg.adaptive_spread_step_bps),
            ("adaptive_max", cfg.adaptive_spread_max_extra_bps),
            ("adaptive_idle", cfg.adaptive_spread_idle_decay_sec),
            ("adaptive_sample_age", cfg.adaptive_spread_sample_max_age_sec),
            ("risk_max_position", self.cfg.risk.max_position_usd),
            ("risk_max_open", self.cfg.risk.max_open_order_usd),
            ("risk_min_order", self.cfg.risk.min_order_notional_usd),
            ("fee_floor", self.fees.projected_net_maker_rate),
            ("budget_soft", self.cfg.runtime.budget_soft_remaining),
            ("budget_hard", self.cfg.runtime.budget_hard_remaining),
            (
                "pause_us_regular",
                int(self.cfg.runtime.pause_during_us_regular_session),
            ),
            (
                "us_pre_buffer",
                self.cfg.runtime.us_regular_session_pre_open_buffer_min,
            ),
            (
                "us_post_buffer",
                self.cfg.runtime.us_regular_session_post_close_buffer_min,
            ),
        )
        values = "|".join(
            f"{name}={value:.8g}" if isinstance(value, float) else f"{name}={value}"
            for name, value in fields
        )
        return f"mm-v4|{values}"

    def _prune_adaptive_samples(self, now_wall: float) -> None:
        cutoff = now_wall - self.cfg.market_maker.adaptive_spread_sample_max_age_sec
        removed = 0
        while self._adaptive_markouts and self._adaptive_markouts[0][2] < cutoff:
            self._adaptive_markouts.popleft()
            removed += 1
        if removed:
            self._adaptive_samples_since_evaluation = min(
                self._adaptive_samples_since_evaluation,
                len(self._adaptive_markouts),
            )

    def _maybe_decay_adaptive_spread(self, now_wall: float) -> bool:
        """Release a no-fill spread lock slowly while the market is calm."""

        cfg = self.cfg.market_maker
        self._prune_adaptive_samples(now_wall)
        if (
            not cfg.adaptive_spread_enabled
            or cfg.adaptive_spread_idle_decay_sec <= 0
            or self._adaptive_extra_bps <= 1e-12
            or self._volatility_guard_active
        ):
            return False
        if (
            cfg.volatility_guard_range_bps > 0
            and self._last_volatility_range_bps
            > cfg.volatility_guard_range_bps * 0.5
        ):
            return False
        reference = max(
            self._adaptive_last_observation_wall,
            self._adaptive_last_decay_wall,
        )
        if reference <= 0:
            self._adaptive_last_decay_wall = now_wall
            return False
        elapsed = now_wall - reference
        if elapsed < cfg.adaptive_spread_idle_decay_sec:
            return False
        decrement = cfg.adaptive_spread_step_bps / 2.0
        steps = max(int(elapsed // cfg.adaptive_spread_idle_decay_sec), 1)
        old_extra = self._adaptive_extra_bps
        self._adaptive_extra_bps = max(old_extra - decrement * steps, 0.0)
        self._adaptive_last_decay_wall = now_wall
        log.info(
            "自适应价差空闲衰减: %.0f 秒无新30秒样本且波动平静；+%.4f -> +%.4f bps",
            elapsed,
            old_extra,
            self._adaptive_extra_bps,
        )
        return self._adaptive_extra_bps < old_extra - 1e-12

    def _update_sigma(self, now: float, mid: float) -> None:
        """EWMA realized volatility from log mid returns, per-second units."""
        if self._sigma_last is not None:
            t0, m0 = self._sigma_last
            dt = now - t0
            if dt >= 0.2 and m0 > 0 and mid > 0:
                r_bps = math.log(mid / m0) * 10_000
                var_rate = (r_bps * r_bps) / dt
                alpha = 1.0 - math.exp(-dt / SIGMA_HALFLIFE_SEC)
                self._sigma_var_bps2 += alpha * (var_rate - self._sigma_var_bps2)
                self._sigma_last = (now, mid)
            elif dt >= 0.2:
                self._sigma_last = (now, mid)
        else:
            self._sigma_last = (now, mid)

    @property
    def sigma_horizon_bps(self) -> float:
        """Expected one-holding-period (60 s) mid move in bps."""
        return math.sqrt(max(self._sigma_var_bps2, 0.0) * SIGMA_HORIZON_SEC)

    def _observe_volatility(self, mid: float) -> float:
        """Track the rolling mid-price range and update the volatility guard.

        One-way moves and violent chop both widen the short-window range.
        Passive ALO quotes can face adverse selection during either pattern.
        While the guard is active, quote only the exposure-reducing side;
        with a flat book, stand aside until the range halves.
        """
        guard = self.cfg.market_maker.volatility_guard_range_bps
        now = time.monotonic()
        self._mid_history.append((now, mid))
        cutoff = now - VOLATILITY_GUARD_WINDOW_SEC
        while self._mid_history and self._mid_history[0][0] < cutoff:
            self._mid_history.popleft()
        range_bps = 0.0
        if len(self._mid_history) >= 2 and mid > 0:
            prices = [px for _, px in self._mid_history]
            range_bps = (max(prices) - min(prices)) / mid * 10_000
        self._last_volatility_range_bps = range_bps
        if guard <= 0:
            self._volatility_guard_active = False
            return range_bps
        if self._volatility_guard_active:
            if range_bps <= guard * 0.5:
                self._volatility_guard_active = False
                log.warning(
                    "波动守卫解除: 60 秒振幅 %.2f bps 已回落到阈值一半以下，恢复双边报价",
                    range_bps,
                )
        elif range_bps >= guard:
            self._volatility_guard_active = True
            log.warning(
                "波动守卫触发: 60 秒振幅 %.2f bps ≥ 阈值 %.2f；只保留减仓方向报价",
                range_bps,
                guard,
            )
        return range_bps

    def on_fill(self, fill: Fill) -> None:
        if fill.order_role.startswith("mm:"):
            self._last_fill_at[fill.is_buy] = time.monotonic()

    def _shock_burst_triggered(self) -> bool:
        """Jump the extra spread to its maximum during a toxic burst."""
        cfg = self.cfg.market_maker
        if len(self._shock_markouts) < (self._shock_markouts.maxlen or 0):
            return False
        if self._adaptive_extra_bps >= cfg.adaptive_spread_max_extra_bps - 1e-12:
            return False
        observed = sum(item[0] for item in self._shock_markouts)
        if observed <= 0:
            return False
        markout_bps = (
            sum(item[1] for item in self._shock_markouts) / observed * 10_000
        )
        wear_bps = self.fees.projected_net_maker_rate * 10_000 - markout_bps
        if wear_bps <= cfg.adaptive_spread_wear_trigger_bps * 2.5:
            return False
        old_extra = self._adaptive_extra_bps
        self._adaptive_extra_bps = cfg.adaptive_spread_max_extra_bps
        self._shock_markouts.clear()
        self._adaptive_samples_since_evaluation = 0
        log.warning(
            "磨损冲击刹车: 最近 %d 样本加权磨损 %.4f bps（阈值 %.4f 的 2.5 倍以上）；"
            "自适应半价差 +%.4f -> +%.4f bps（直接拉满，磨损回落后自动收窄）",
            self._shock_markouts.maxlen,
            wear_bps,
            cfg.adaptive_spread_wear_trigger_bps,
            old_extra,
            self._adaptive_extra_bps,
        )
        return True

    def on_markout(self, markout: dict) -> bool:
        cfg = self.cfg.market_maker
        if not cfg.adaptive_spread_enabled or int(markout.get("horizon_sec") or 0) != 30:
            return False
        # Keep risk exits in the overall wear ledger, but never teach the
        # passive quote controller from taker/flatten slippage.  New rows carry
        # both fields; missing metadata remains accepted for legacy ledgers.
        role = str(markout.get("order_role") or "")
        liquidity = str(markout.get("liquidity") or "").lower()
        if (role and not role.startswith("mm:")) or (
            liquidity and liquidity != "maker"
        ):
            return False
        try:
            notional = float(markout.get("notional_usd") or 0.0)
            pnl = float(markout.get("markout_pnl_usd") or 0.0)
        except (TypeError, ValueError, OverflowError):
            return False
        if notional <= 0 or not math.isfinite(notional) or not math.isfinite(pnl):
            return False
        now_wall = time.time()
        self._prune_adaptive_samples(now_wall)
        self._adaptive_markouts.append((notional, pnl, now_wall))
        self._shock_markouts.append((notional, pnl))
        self._adaptive_last_observation_wall = now_wall
        self._adaptive_last_decay_wall = now_wall
        self._adaptive_samples_since_evaluation += 1
        if self._shock_burst_triggered():
            return True
        if (
            len(self._adaptive_markouts) < cfg.adaptive_spread_min_samples
            or self._adaptive_samples_since_evaluation
            < cfg.adaptive_spread_adjust_every_samples
        ):
            return False
        self._adaptive_samples_since_evaluation = 0
        observed = sum(item[0] for item in self._adaptive_markouts)
        markout_pnl = sum(item[1] for item in self._adaptive_markouts)
        weighted_markout_bps = markout_pnl / observed * 10_000
        projected_wear_bps = (
            self.fees.projected_net_maker_rate * 10_000 - weighted_markout_bps
        )
        if projected_wear_bps <= cfg.adaptive_spread_wear_trigger_bps:
            # Wear comfortably back under control: step the extra spread down
            # so quoting re-tightens and volume recovers instead of staying at
            # storm width forever.  Half steps keep the decay slower than the
            # widening path, and the shock brake can always re-fire.
            if (
                self._adaptive_extra_bps > 1e-12
                and projected_wear_bps
                <= cfg.adaptive_spread_wear_trigger_bps * 0.5
            ):
                old_extra = self._adaptive_extra_bps
                self._adaptive_extra_bps = max(
                    old_extra - cfg.adaptive_spread_step_bps / 2.0, 0.0
                )
                log.info(
                    "30秒成交磨损 %.4f bps 已回落到阈值一半以下；"
                    "自适应半价差 +%.4f -> +%.4f bps（逐步收窄）",
                    projected_wear_bps,
                    old_extra,
                    self._adaptive_extra_bps,
                )
                return True
            return False
        old_extra = self._adaptive_extra_bps
        self._adaptive_extra_bps = min(
            old_extra + cfg.adaptive_spread_step_bps,
            cfg.adaptive_spread_max_extra_bps,
        )
        if self._adaptive_extra_bps <= old_extra + 1e-12:
            return False
        log.warning(
            "30秒成交磨损 %.4f bps 超过阈值 %.4f；自适应半价差 +%.4f -> +%.4f bps（%d 样本）",
            projected_wear_bps,
            cfg.adaptive_spread_wear_trigger_bps,
            old_extra,
            self._adaptive_extra_bps,
            len(self._adaptive_markouts),
        )
        return True

    def state_dict(self) -> dict:
        return {
            "adaptive_regime_key": self._adaptive_regime_key(),
            "adaptive_extra_bps": self._adaptive_extra_bps,
            "adaptive_samples_since_evaluation": self._adaptive_samples_since_evaluation,
            "adaptive_markouts": [list(item) for item in self._adaptive_markouts],
            "adaptive_last_observation_wall": self._adaptive_last_observation_wall,
            "adaptive_last_decay_wall": self._adaptive_last_decay_wall,
        }

    def load_state(self, state: dict) -> None:
        cfg = self.cfg.market_maker
        self._adaptive_markouts.clear()
        self._adaptive_samples_since_evaluation = 0
        self._adaptive_extra_bps = 0.0
        self._adaptive_last_observation_wall = 0.0
        self._adaptive_last_decay_wall = 0.0
        saved_regime = str(state.get("adaptive_regime_key") or "")
        if saved_regime != self._adaptive_regime_key():
            if state.get("adaptive_extra_bps") or state.get("adaptive_markouts"):
                log.info("报价制度已变化；丢弃旧制度的自适应价差与 markout 窗口")
            return
        # Disabled means disabled: even a manually edited/corrupt same-regime
        # state must not keep a permanent extra spread that can never decay.
        if not cfg.adaptive_spread_enabled:
            return
        try:
            extra = float(state.get("adaptive_extra_bps") or 0.0)
        except (TypeError, ValueError, OverflowError):
            extra = 0.0
        self._adaptive_extra_bps = _clamp(
            extra if math.isfinite(extra) else 0.0,
            0.0,
            cfg.adaptive_spread_max_extra_bps,
        )
        try:
            since = int(state.get("adaptive_samples_since_evaluation") or 0)
        except (TypeError, ValueError, OverflowError):
            since = 0
        self._adaptive_samples_since_evaluation = max(since, 0)
        try:
            last_observation = float(
                state.get("adaptive_last_observation_wall") or 0.0
            )
            last_decay = float(state.get("adaptive_last_decay_wall") or 0.0)
        except (TypeError, ValueError, OverflowError):
            last_observation = 0.0
            last_decay = 0.0
        self._adaptive_last_observation_wall = (
            last_observation if math.isfinite(last_observation) else 0.0
        )
        self._adaptive_last_decay_wall = (
            last_decay if math.isfinite(last_decay) else 0.0
        )
        rows = state.get("adaptive_markouts") or []
        if not isinstance(rows, list):
            return
        for row in rows[-cfg.adaptive_spread_window_samples :]:
            if not isinstance(row, (list, tuple)) or len(row) not in {2, 3}:
                continue
            try:
                notional, pnl = float(row[0]), float(row[1])
                observed_wall = (
                    float(row[2])
                    if len(row) == 3
                    else self._adaptive_last_observation_wall
                )
            except (TypeError, ValueError, OverflowError):
                continue
            if (
                notional > 0
                and math.isfinite(notional)
                and math.isfinite(pnl)
                and math.isfinite(observed_wall)
            ):
                self._adaptive_markouts.append((notional, pnl, observed_wall))
        self._prune_adaptive_samples(time.time())

    def _crisis_quotes(
        self, market: MarketSnapshot, account: AccountState
    ) -> list[OrderIntent]:
        """Far-touch mean-reversion quotes for a dislocated book.

        With no competing makers, passive liquidity can demand an extreme
        premium: park small ALO orders around the live oracle anchor and let
        panic flow sweep into them; once holding inventory, work the exit
        near the oracle to harvest the reversion.  Sizing is deliberately a
        fraction of normal quoting and capped at ~2 fills per side.
        """
        cfg = self.cfg.market_maker
        notional = cfg.crisis_quote_notional_usd
        oracle = market.oracle_px
        book = market.book
        if (
            notional < self.cfg.risk.min_order_notional_usd
            or not oracle
            or book.best_bid is None
            or book.best_ask is None
        ):
            return []
        distance = cfg.crisis_quote_distance_bps / 10_000
        # Exit edge: enough to clear projected fees plus a real capture.
        exit_edge = max(
            self.fees.projected_net_maker_rate + 0.0005,
            cfg.min_edge_after_projected_rebate_bps / 10_000,
        )
        position_usd = account.position_qty * oracle
        cap = notional * 2.0
        raw: list[OrderIntent] = []
        if position_usd >= cap:
            # Long from catching a down-sweep: work the reversion exit only.
            exit_px = market.meta.px_round(
                oracle * (1.0 + exit_edge), round_up=True
            )
            if exit_px > book.best_bid:
                qty = market.meta.qty_floor(
                    min(abs(account.position_qty), notional * 2.0 / exit_px)
                )
                if qty > 0:
                    raw.append(
                        OrderIntent(
                            "mm:ask:0", False, exit_px, qty, reduce_only=True
                        )
                    )
            return self._allocate(raw, market, account)
        if position_usd <= -cap:
            exit_px = market.meta.px_round(
                oracle * (1.0 - exit_edge), round_up=False
            )
            if exit_px < book.best_ask:
                qty = market.meta.qty_floor(
                    min(abs(account.position_qty), notional * 2.0 / exit_px)
                )
                if qty > 0:
                    raw.append(
                        OrderIntent(
                            "mm:bid:0", True, exit_px, qty, reduce_only=True
                        )
                    )
            return self._allocate(raw, market, account)
        bid_px = market.meta.px_round(oracle * (1.0 - distance), round_up=False)
        ask_px = market.meta.px_round(oracle * (1.0 + distance), round_up=True)
        qty_buy = market.meta.qty_floor(notional / bid_px)
        qty_sell = market.meta.qty_floor(notional / ask_px)
        # ALO safety: never cross whatever book remains.
        if bid_px < book.best_ask and qty_buy > 0:
            raw.append(OrderIntent("mm:bid:0", True, bid_px, qty_buy))
        if ask_px > book.best_bid and qty_sell > 0:
            raw.append(OrderIntent("mm:ask:0", False, ask_px, qty_sell))
        return self._allocate(raw, market, account)

    def _allocate(
        self,
        raw: list[OrderIntent],
        market: MarketSnapshot,
        account: AccountState,
    ) -> list[OrderIntent]:
        return _allocate_intents(
            raw,
            account=account,
            fair=market.fair_px or market.oracle_px or 0.0,
            max_position_usd=self.cfg.risk.max_position_usd,
            max_open_order_usd=self.cfg.risk.max_open_order_usd,
            min_notional=self.cfg.risk.min_order_notional_usd,
            qty_floor=market.meta.qty_floor,
            entry_limit_usd=(
                self.cfg.risk.max_position_usd
                * self.cfg.market_maker.rebalance_at_position_ratio
            ),
        )

    def desired(self, market: MarketSnapshot, account: AccountState) -> list[OrderIntent]:
        if self.crisis_mode:
            return self._crisis_quotes(market, account)
        fair = market.fair_px
        book = market.book
        if fair is None or book.best_bid is None or book.best_ask is None:
            return []
        cfg = self.cfg.market_maker
        projected_fee_bps = self.fees.projected_net_maker_rate * 10_000
        required_half = projected_fee_bps + cfg.min_edge_after_projected_rebate_bps / 2.0
        position_usd = account.position_qty * fair
        ratio = _clamp(position_usd / self.cfg.risk.max_position_usd, -1.0, 1.0)
        # Quote center: a raw top-level microprice is dangerous on thin HIP-3
        # books because our own resting size can move it.  Blend it only by the
        # reviewed weight; the optimized live profile uses the plain mid.
        mid = book.mid or fair
        microprice = book.microprice or mid
        quote_center = mid + cfg.microprice_weight * (microprice - mid)
        self._observe_volatility(mid)
        self._update_sigma(time.monotonic(), mid)
        self._maybe_decay_adaptive_spread(time.time())
        # Avellaneda-Stoikov structure: the configured half-spread acts as a
        # floor and realized volatility widens quoting continuously (instead
        # of only after wear is measured); the inventory skew unit likewise
        # scales with sigma so a risky book pushes inventory out harder.
        sigma_h = self.sigma_horizon_bps
        vol_spread = cfg.vol_spread_mult * sigma_h if cfg.vol_spread_mult > 0 else 0.0
        # Volatility and realized markout describe overlapping toxicity.  Use
        # the larger protection instead of adding both and starving the quote
        # after the same event has already widened each component.
        base_plus_adaptive = cfg.half_spread_bps + self._adaptive_extra_bps
        half = max(base_plus_adaptive, vol_spread, required_half)
        # Trend / volatility protection.  The exposure-increasing side stops
        # quoting at heavy inventory and during a volatility burst; the
        # exposure-reducing side keeps working the exit.  With a flat book in
        # a burst, both sides stand aside until the range halves.
        block_buy = ratio >= cfg.rebalance_at_position_ratio
        block_sell = ratio <= -cfg.rebalance_at_position_ratio
        if self._volatility_guard_active:
            if ratio >= 0:
                block_buy = True
            if ratio <= 0:
                block_sell = True
        cooldown = cfg.same_side_fill_cooldown_sec
        if cooldown > 0:
            now = time.monotonic()
            if ratio >= 0 and now - self._last_fill_at[True] < cooldown:
                block_buy = True
            if ratio <= 0 and now - self._last_fill_at[False] < cooldown:
                block_sell = True
        vol_skew = cfg.vol_skew_mult * sigma_h if cfg.vol_skew_mult > 0 else 0.0
        skew_unit = max(
            cfg.inventory_skew_bps_at_limit,
            min(vol_skew, cfg.inventory_skew_bps_at_limit * 3.0),
        )
        center = quote_center * (1.0 - ratio * skew_unit / 10_000)
        spacing = max(cfg.level_spacing_bps, 0.6 * sigma_h)
        raw: list[OrderIntent] = []
        used_buy_prices: set[float] = set()
        used_sell_prices: set[float] = set()
        # At meaningful inventory, prioritize a passive exit over another
        # stale-fair quote.  Entropy equity mark/oracle values can lag the
        # live book by several bps; pegging only the exposure-reducing side
        # to the BBO restores turnover without ever crossing the spread.
        rebalance_buy = ratio <= -cfg.rebalance_at_position_ratio
        rebalance_sell = ratio >= cfg.rebalance_at_position_ratio
        now = time.monotonic()
        if rebalance_buy or rebalance_sell:
            if self._rebalance_breach_since is None:
                self._rebalance_breach_since = now
        else:
            self._rebalance_breach_since = None
        exit_aged = (
            self._rebalance_breach_since is not None
            and now - self._rebalance_breach_since >= INVENTORY_IMPROVE_AFTER_SEC
        )
        # order_notional_usd is one side's TOTAL ladder budget.  Extra levels
        # split it instead of multiplying exposure and actions.
        raw_weights = [cfg.level_notional_decay**level for level in range(cfg.levels)]
        weight_total = sum(raw_weights)
        level_notionals = [
            cfg.order_notional_usd * weight / weight_total for weight in raw_weights
        ]
        if rebalance_buy and not block_buy:
            # Fresh breach: queue at our own bid (free).  Aged breach: jump to
            # the front of the spread so the next market sell hits us first.
            exit_px = (
                market.meta.px_round(
                    book.best_ask * (1.0 - 1e-5), round_up=False
                )
                if exit_aged
                else book.best_bid
            )
            exit_px = max(exit_px, book.best_bid)
            qty = market.meta.qty_floor(abs(account.position_qty))
            if exit_px < book.best_ask and qty > 0:
                raw.append(
                    OrderIntent("mm:bid:0", True, exit_px, qty, reduce_only=True)
                )
                used_buy_prices.add(exit_px)
        elif rebalance_sell and not block_sell:
            exit_px = (
                market.meta.px_round(
                    book.best_bid * (1.0 + 1e-5), round_up=True
                )
                if exit_aged
                else book.best_ask
            )
            exit_px = min(exit_px, book.best_ask)
            qty = market.meta.qty_floor(abs(account.position_qty))
            if exit_px > book.best_bid and qty > 0:
                raw.append(
                    OrderIntent("mm:ask:0", False, exit_px, qty, reduce_only=True)
                )
                used_sell_prices.add(exit_px)
        for level in range(cfg.levels):
            distance = half + level * spacing
            buy_px0 = market.meta.px_round(
                center * (1.0 - distance / 10_000), round_up=False
            )
            sell_px0 = market.meta.px_round(
                center * (1.0 + distance / 10_000), round_up=True
            )
            # Mark/oracle may lag the live book for equity perps. Keep both
            # sides present without crossing: a stale-low fair must not turn a
            # neutral maker into an unbounded buy-only strategy (and vice versa).
            buy_px = min(buy_px0, book.best_bid)
            sell_px = max(sell_px0, book.best_ask)
            level_notional = level_notionals[level]
            qty_buy = market.meta.qty_floor(level_notional / buy_px)
            qty_sell = market.meta.qty_floor(level_notional / sell_px)
            if (
                not block_buy
                and not rebalance_buy
                and buy_px < book.best_ask
                and buy_px not in used_buy_prices
                and qty_buy > 0
            ):
                raw.append(
                    OrderIntent(
                        f"mm:bid:{level}",
                        True,
                        buy_px,
                        qty_buy,
                        reduce_only=False,
                    )
                )
                used_buy_prices.add(buy_px)
            if (
                not block_sell
                and not rebalance_sell
                and sell_px > book.best_bid
                and sell_px not in used_sell_prices
                and qty_sell > 0
            ):
                raw.append(
                    OrderIntent(
                        f"mm:ask:{level}",
                        False,
                        sell_px,
                        qty_sell,
                        reduce_only=False,
                    )
                )
                used_sell_prices.add(sell_px)
        # Closest quotes first, then alternate sides in the natural list order.
        allocated = _allocate_intents(
            raw,
            account=account,
            fair=fair,
            max_position_usd=self.cfg.risk.max_position_usd,
            max_open_order_usd=self.cfg.risk.max_open_order_usd,
            min_notional=self.cfg.risk.min_order_notional_usd,
            qty_floor=market.meta.qty_floor,
            entry_limit_usd=(
                self.cfg.risk.max_position_usd
                * cfg.rebalance_at_position_ratio
            ),
        )
        level0_bid = next(
            (item.price for item in allocated if item.role == "mm:bid:0"), None
        )
        level0_ask = next(
            (item.price for item in allocated if item.role == "mm:ask:0"), None
        )
        self._last_quote_metrics = {
            "base_half_spread_bps": cfg.half_spread_bps,
            "fee_floor_half_spread_bps": required_half,
            "vol_spread_component_bps": vol_spread,
            "effective_half_spread_bps": half,
            "effective_spacing_bps": spacing,
            "inventory_ratio": ratio,
            "inventory_skew_unit_bps": skew_unit,
            "quote_center_px": quote_center,
            "mid_px": mid,
            "microprice_px": microprice,
            "microprice_weight": cfg.microprice_weight,
            "level0_bid_distance_bps": (
                (mid - level0_bid) / mid * 10_000 if level0_bid else None
            ),
            "level0_ask_distance_bps": (
                (level0_ask - mid) / mid * 10_000 if level0_ask else None
            ),
            "levels": cfg.levels,
            "per_side_ladder_budget_usd": cfg.order_notional_usd,
            "block_exposure_increasing_buy": block_buy,
            "block_exposure_increasing_sell": block_sell,
        }
        return allocated


class GridStrategy(Strategy):
    name = "grid"

    def __init__(self, cfg: Config, fees: FeeModel) -> None:
        super().__init__(cfg, fees)
        self.anchor: float | None = cfg.grid.anchor_price
        self.blocked: dict[int, dict] = {}
        self.created_ts = time.time()
        required = (
            2.0 * max(fees.gross_maker_rate, 0.0) * 10_000
            + cfg.grid.min_profit_after_projected_rebate_bps
        )
        if cfg.grid.spacing_bps + 1e-9 < required:
            raise ValueError(
                f"grid.spacing_bps={cfg.grid.spacing_bps:.4f} 低于实际手续费后最低要求 "
                f"{required:.4f} bps"
            )

    def _price(self, index: int) -> float:
        assert self.anchor is not None
        return self.anchor * (1.0 + index * self.cfg.grid.spacing_bps / 10_000)

    def _rearm(self, fair: float) -> None:
        threshold = self.cfg.grid.level_rearm_bps / 10_000
        for index, item in list(self.blocked.items()):
            px = float(item["price"])
            side = item["side"]
            if (side == "buy" and fair >= px * (1.0 + threshold)) or (
                side == "sell" and fair <= px * (1.0 - threshold)
            ):
                self.blocked.pop(index, None)
                log.info("网格层 %s 已重新激活", index)

    def _maybe_recenter(self, fair: float, account: AccountState) -> None:
        if self.anchor is None:
            self.anchor = fair
            return
        distance = abs(fair / self.anchor - 1.0) * 10_000
        # A small real inventory is still inventory; do not silently move its grid.
        flat = abs(account.position_qty) < 1e-12
        if self.cfg.grid.anchor_price is None and flat and not self.blocked and distance >= self.cfg.grid.recenter_when_flat_bps:
            old = self.anchor
            self.anchor = fair
            self.created_ts = time.time()
            log.info("账户空仓，网格中心重置 %.6f -> %.6f", old, fair)

    def desired(self, market: MarketSnapshot, account: AccountState) -> list[OrderIntent]:
        fair = market.fair_px
        book = market.book
        if fair is None or book.best_bid is None or book.best_ask is None:
            return []
        direction = 1 if self.cfg.grid.inventory_direction == "long" else -1
        if account.position_qty * direction < -market.meta.size_step / 2:
            return []
        self._maybe_recenter(fair, account)
        self._rearm(fair)
        assert self.anchor is not None
        raw: list[OrderIntent] = []
        n = self.cfg.grid.levels_each_side
        for index in range(-n, n + 1):
            if index == 0 or index in self.blocked:
                continue
            px0 = self._price(index)
            if px0 < fair:
                px = market.meta.px_round(px0, round_up=False)
                is_buy = True
                if px >= book.best_ask:
                    continue
            elif px0 > fair:
                px = market.meta.px_round(px0, round_up=True)
                is_buy = False
                if px <= book.best_bid:
                    continue
            else:
                continue
            qty = market.meta.qty_floor(self.cfg.grid.order_notional_usd / px)
            if qty > 0:
                reducing = is_buy != (direction > 0)
                raw.append(OrderIntent(f"grid:{index}", is_buy, px, qty, reduce_only=reducing))
        # Reserve exits first; every exit is backed by the actual signed position.
        # Budgets are in quantity at the current mark, not at each limit price:
        # several cheaper bids otherwise jointly exceed the position cap.
        raw.sort(key=lambda x: (not x.reduce_only, abs(x.price - fair)))
        exit_room = max(account.position_qty * direction, 0.0)
        entry_room = max(self.cfg.risk.max_position_usd / fair - exit_room, 0.0)
        open_room = self.cfg.risk.max_open_order_usd
        result = []
        for item in raw:
            room = exit_room if item.reduce_only else entry_room
            qty = market.meta.qty_floor(min(item.qty, room, open_room / item.price))
            if qty <= 0 or qty * item.price + 1e-9 < self.cfg.risk.min_order_notional_usd:
                continue
            item.qty = qty
            result.append(item)
            if item.reduce_only:
                exit_room = max(exit_room - qty, 0.0)
            else:
                entry_room = max(entry_room - qty, 0.0)
            open_room = max(open_room - item.notional, 0.0)
        return result

    def on_fill(self, fill: Fill) -> None:
        if not fill.order_role.startswith("grid:"):
            return
        try:
            index = int(fill.order_role.split(":", 1)[1])
        except ValueError:
            return
        self.blocked[index] = {
            "side": "buy" if fill.is_buy else "sell",
            "price": fill.price,
            "time_ms": fill.time_ms,
        }

    def halt_reason(self, market: MarketSnapshot, account: AccountState) -> str | None:
        direction = 1 if self.cfg.grid.inventory_direction == "long" else -1
        if account.position_qty * direction < -market.meta.size_step / 2:
            return "真实持仓方向与网格方向不符；请平仓并撤清挂单后切换方向"
        if self.anchor is None or market.fair_px is None:
            return None
        n = self.cfg.grid.levels_each_side
        low = self._price(-n) * (1.0 - self.cfg.grid.breakout_buffer_bps / 10_000)
        high = self._price(n) * (1.0 + self.cfg.grid.breakout_buffer_bps / 10_000)
        if market.fair_px < low:
            return f"价格 {market.fair_px:.6f} 跌破网格保护下界 {low:.6f}"
        if market.fair_px > high:
            return f"价格 {market.fair_px:.6f} 突破网格保护上界 {high:.6f}"
        return None

    def state_dict(self) -> dict:
        return {
            "inventory_direction": self.cfg.grid.inventory_direction,
            "anchor": self.anchor,
            "blocked": {str(k): v for k, v in self.blocked.items()},
            "created_ts": self.created_ts,
        }

    def load_state(self, state: dict) -> None:
        # Old neutral-grid levels and opposite-direction cooldowns are not reusable.
        if state.get("inventory_direction") != self.cfg.grid.inventory_direction:
            return
        anchor = state.get("anchor")
        if self.cfg.grid.anchor_price is not None and anchor != self.cfg.grid.anchor_price:
            # An explicit user edit takes precedence over the previous run.
            self.anchor = self.cfg.grid.anchor_price
            self.blocked.clear()
            return
        if isinstance(anchor, (int, float)) and anchor > 0:
            self.anchor = float(anchor)
        blocked = state.get("blocked") or {}
        if isinstance(blocked, dict):
            self.blocked = {
                int(k): v
                for k, v in blocked.items()
                if str(k).lstrip("-").isdigit() and isinstance(v, dict)
            }
        if isinstance(state.get("created_ts"), (int, float)):
            self.created_ts = float(state["created_ts"])


def make_strategy(cfg: Config, fees: FeeModel) -> Strategy:
    if cfg.strategy == "market_maker":
        return MarketMakerStrategy(cfg, fees)
    return GridStrategy(cfg, fees)
