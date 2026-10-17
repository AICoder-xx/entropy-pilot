"""Local conservative paper execution and accounting."""
from __future__ import annotations

from .broker_base import Broker
from .config import Config
from .models import FeeModel
from .models import Fill
from .models import MarketMeta
from .models import OrderBook
from .models import OrderIntent
from .models import TrackedOrder
from typing import Iterable
from typing import Optional
import time
import logging

log = logging.getLogger("entropy.broker")

class PaperBroker(Broker):
    def __init__(self, cfg: Config, meta: MarketMeta, fees: FeeModel) -> None:
        super().__init__(cfg, meta, fees)
        self.account.equity_actual = cfg.paper.initial_equity_usd
        self._initial_equity = cfg.paper.initial_equity_usd
        self._fill_seq = 0

    async def start(self) -> None:
        log.info("模拟盘已启动；保守成交模型 touch_fill=%s", self.cfg.paper.touch_fill)

    async def place(self, intents: list[OrderIntent], book: OrderBook) -> None:
        if book.best_bid is None or book.best_ask is None:
            raise RuntimeError("空盘口，拒绝下单")
        for intent in intents:
            if intent.tif == "Alo":
                if intent.is_buy and intent.price >= book.best_ask:
                    raise RuntimeError(f"ALO 买单会吃单: {intent.price} >= {book.best_ask}")
                if not intent.is_buy and intent.price <= book.best_bid:
                    raise RuntimeError(f"ALO 卖单会吃单: {intent.price} <= {book.best_bid}")
            cloid = self.cloids.next()
            if self.order_intent_sink is not None:
                self._note_order_event(
                    "place", durable_intent=True,
                    role=intent.role, cloid=cloid, oid=int(cloid[-12:], 16),
                    is_buy=intent.is_buy, px=intent.price, qty=intent.qty,
                    reduce_only=intent.reduce_only, tif=intent.tif,
                    outcome="submitted",
                )
            self.orders_by_role[intent.role] = TrackedOrder(
                role=intent.role,
                cloid=cloid,
                is_buy=intent.is_buy,
                price=intent.price,
                qty=intent.qty,
                reduce_only=intent.reduce_only,
                tif=intent.tif,
                created_ts=time.time(),
                oid=int(cloid[-12:], 16),
            )

    async def cancel_roles(self, roles: Iterable[str]) -> None:
        for role in list(roles):
            self.orders_by_role.pop(role, None)

    def _apply_position(self, is_buy: bool, qty: float, px: float) -> float:
        state = self.account
        old = state.position_qty
        signed = qty if is_buy else -qty
        if old == 0 or old * signed > 0:
            new = old + signed
            state.entry_px = (
                (abs(old) * state.entry_px + qty * px) / abs(new) if new else 0.0
            )
            state.position_qty = new
            return 0.0

        closing = min(abs(old), qty)
        pnl = closing * (px - state.entry_px) * (1.0 if old > 0 else -1.0)
        new = old + signed
        state.position_qty = new
        if new == 0:
            state.entry_px = 0.0
        elif old * new < 0:
            state.entry_px = px
        state.realized_pnl += pnl
        return pnl

    def _fill(self, order: TrackedOrder, px: float, qty: Optional[float] = None) -> None:
        state = self.account
        fill_qty = qty if qty is not None else order.qty - order.filled_qty
        if order.reduce_only:
            if state.position_qty > 0 and not order.is_buy:
                fill_qty = min(fill_qty, state.position_qty)
            elif state.position_qty < 0 and order.is_buy:
                fill_qty = min(fill_qty, abs(state.position_qty))
            else:
                fill_qty = 0.0
        if fill_qty <= 0:
            self.orders_by_role.pop(order.role, None)
            return
        start_position_qty = state.position_qty
        pnl = self._apply_position(order.is_buy, fill_qty, px)
        notional = fill_qty * px
        rate = self.fees.gross_maker_rate if order.tif == "Alo" else self.fees.gross_taker_rate
        fee = notional * rate
        own, referred = self.fees.projected_entropy_rebate(fee)
        state.fees_paid += fee
        state.projected_self_rebate += own
        state.projected_referred_benefit += referred
        state.volume_usd += notional
        order.filled_qty += fill_qty
        self._fill_seq += 1
        fill = Fill(
            fill_id=f"paper-{order.cloid}-{self._fill_seq}",
            order_role=order.role,
            cloid=order.cloid,
            oid=order.oid,
            is_buy=order.is_buy,
            price=px,
            qty=fill_qty,
            fee_usd=fee,
            liquidity="maker" if order.tif == "Alo" else "taker",
            time_ms=int(time.time() * 1000),
            realized_pnl_delta=pnl,
            projected_self_rebate=own,
            projected_referred_benefit=referred,
            start_position_qty=start_position_qty,
        )
        self._fills.append(fill)
        self.orders_by_role.pop(order.role, None)

    async def sync(self, book: OrderBook) -> None:
        if book.best_bid is None or book.best_ask is None or book.mid is None:
            return
        for order in list(self.orders_by_role.values()):
            if self.cfg.paper.touch_fill:
                hit = (
                    order.price >= book.best_bid if order.is_buy
                    else order.price <= book.best_ask
                )
            else:
                hit = (
                    order.price >= book.best_ask if order.is_buy
                    else order.price <= book.best_bid
                )
            if hit:
                self._fill(order, order.price)
        state = self.account
        state.unrealized_pnl = (
            state.position_qty * (book.mid - state.entry_px)
            if state.position_qty and state.entry_px
            else 0.0
        )
        state.equity_actual = (
            self._initial_equity + state.realized_pnl + state.unrealized_pnl - state.fees_paid
        )
        state.withdrawable = max(
            state.equity_actual - abs(state.position_qty * book.mid), 0.0
        )

    async def flatten(self, book: OrderBook, slippage_bps: float) -> None:
        if not self.account.position_qty:
            return
        if book.best_bid is None or book.best_ask is None:
            raise RuntimeError("空盘口，无法紧急平仓")
        is_buy = self.account.position_qty < 0
        px = book.best_ask if is_buy else book.best_bid
        order = TrackedOrder(
            role="emergency-flatten",
            cloid=self.cloids.next(),
            is_buy=is_buy,
            price=px,
            qty=abs(self.account.position_qty),
            reduce_only=True,
            tif="Ioc",
            created_ts=time.time(),
        )
        self._fill(order, px)
