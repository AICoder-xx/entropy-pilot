from __future__ import annotations

import abc
import asyncio
import ctypes
import hashlib
import logging
import math
import os
import time
from collections import deque
from dataclasses import replace
from typing import Callable, Iterable, Optional

from .config import Config
from .market import MarketClient
from .models import (
    AccountState,
    FeeModel,
    Fill,
    MarketMeta,
    OrderBook,
    OrderIntent,
    TrackedOrder,
)
from .pacing import ActionBudget

log = logging.getLogger("entropy.broker")

# Push-based views are only trusted while the shared websocket is visibly
# alive (l2Book pushes at least every block, so silence beyond this window
# means the connection is degraded and REST must take over).
WS_STALE_FALLBACK_SEC = 10.0
# How long _cancel_cloids waits for orderUpdates to confirm terminal states
# before falling back to one authoritative REST open-orders read.
WS_CANCEL_CONFIRM_SEC = 1.5
# A timed-out cancel is safe to resend only after a fresh authoritative
# open-orders snapshot proves that the exact Cloid is still resting.  Keep
# both the cadence and the number of signing attempts bounded; the wider
# reconciliation grace remains the final fail-closed deadline.
CANCEL_RETRY_INTERVAL_SEC = 5.0
CANCEL_MAX_SUBMIT_ATTEMPTS = 3

BOT_CLOID_MAGIC = 0x0E170B07
CANCELED_ORDER_STATES = {
    "canceled",
    "cancelled",
    "rejected",
    "margincanceled",
    "vaultwithdrawalcanceled",
    "openinterestcapcanceled",
    "selftradecanceled",
    "reduceonlycanceled",
    "siblingfilledcanceled",
    "delistedcanceled",
    "liquidatedcanceled",
    "scheduledcancel",
    "tickrejected",
    "mintradentlrejected",
    "perpmarginrejected",
    "reduceonlyrejected",
    "badalopxrejected",
    "ioccancelrejected",
    "badtriggerpxrejected",
    "marketordernoliquidityrejected",
    "positionincreaseatopeninterestcaprejected",
    "positionflipatopeninterestcaprejected",
    "tooaggressiveatopeninterestcaprejected",
    "openinterestincreaserejected",
    "insufficientspotbalancerejected",
    "oraclerejected",
    "perpmaxpositionrejected",
}
# Keep official-view contradictions bounded at one minute, but allow the
# separately paginated userFills feed longer to catch up after orderStatus has
# already reported a fill.  Entropy/HIP-3 has shown repeatable 60-120 second
# detail lag; during this window the role mapping stays reserved, so extending
# it cannot create a duplicate replacement order.
ORDER_RECONCILIATION_GRACE_SEC = 60.0
FILL_RECONCILIATION_GRACE_SEC = 300.0
RETIRED_ORDER_FILL_GRACE_SEC = 30 * 60.0
STARTUP_EXTERNAL_ACTIVITY_QUIET_SEC = 15 * 60


class PendingOrderReconciliation(RuntimeError):
    """Official order views disagree but the strategy can wait safely."""


class PendingFillReconciliation(PendingOrderReconciliation):
    """The exchange reports a fill whose userFills row is not visible yet."""


class PendingCancelReconciliation(PendingOrderReconciliation):
    """A bounded cancel attempt is still reported open by official state."""


class UncertainIocOutcome(RuntimeError):
    """An IOC send cannot yet be proven accepted or rejected.

    Callers must not submit another IOC while this exception is unresolved.
    ``LiveBroker`` retains the cloid locally and rechecks it read-only before
    allowing any later IOC action.
    """


def _http_status_code(exc: BaseException) -> int:
    """Return an HTTP status carried by SDK/aiohttp exception chains."""

    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        for name in ("status_code", "status"):
            try:
                value = int(getattr(current, name, 0) or 0)
            except (TypeError, ValueError):
                value = 0
            if value:
                return value
        current = current.__cause__ or current.__context__
    return 0


def _order_state(status: dict | None) -> str:
    order_info = (status or {}).get("order") or {}
    value = str(order_info.get("status") or "").lower()
    return value.replace("-", "").replace("_", "")


def _reported_filled_qty(status: dict | None) -> float:
    """Return exchange-reported cumulative fill for an orderStatus payload."""
    if not status or status.get("status") != "order":
        return 0.0
    outer = status.get("order") or {}
    inner = outer.get("order") or {}
    try:
        return max(
            float(inner.get("origSz") or 0.0) - float(inner.get("sz") or 0.0),
            0.0,
        )
    except (TypeError, ValueError):
        return 0.0


def _positive_order_id(raw: object) -> int | None:
    if isinstance(raw, bool):
        return None
    try:
        value = int(raw)
    except (TypeError, ValueError, OverflowError):
        return None
    return value if value > 0 and str(value) == str(raw) else None


def recovered_order_identity(status: dict | None, cloid: str, coin: str) -> int | None:
    """Require positive, matching identity evidence before releasing a restart gate."""
    if not isinstance(status, dict) or status.get("status") != "order":
        return None
    outer = status.get("order")
    inner = outer.get("order") if isinstance(outer, dict) else None
    if not isinstance(inner, dict):
        return None
    if str(inner.get("cloid") or "").lower() != cloid.lower() or inner.get("coin") != coin:
        return None
    return _positive_order_id(inner.get("oid"))


def allocation_state(account: AccountState) -> tuple:
    """Values which invalidate an allocation after awaiting an order mutation."""
    return (
        account.position_qty, account.entry_px, account.volume_usd,
        account.equity_actual, account.available_to_buy_usd, account.available_to_sell_usd,
    )


def _validated_positions(state: dict) -> list[dict]:
    if not isinstance(state, dict) or not isinstance(state.get("assetPositions"), list):
        raise RuntimeError("官方仓位响应缺少 assetPositions；不能解释为空仓")
    positions = state["assetPositions"]
    for item in positions:
        position = item.get("position") if isinstance(item, dict) else None
        if not isinstance(position, dict) or not position.get("coin"):
            raise RuntimeError("官方仓位行缺少市场或 position")
        for field in ("szi", "entryPx", "unrealizedPnl", "liquidationPx"):
            raw = position.get(field)
            if field != "szi" and raw in (None, ""):
                continue
            if raw in (None, "") or isinstance(raw, bool) or not math.isfinite(float(raw)):
                raise RuntimeError(f"官方仓位 {field} 无效；保留上次仓位")
        if float(position["szi"]) != 0 and float(position.get("entryPx") or 0) <= 0:
            raise RuntimeError("官方非零仓位缺少有效持仓成本")
    return positions


def is_bot_cloid(value: object) -> bool:
    try:
        return int(str(value), 16) >> 96 == BOT_CLOID_MAGIC
    except (TypeError, ValueError):
        return False


def order_ownership(status: dict | None) -> tuple[str, str | None]:
    """Classify an authoritative orderStatus payload by Cloid namespace."""
    if not isinstance(status, dict) or status.get("status") != "order":
        return "unknown", None
    outer = status.get("order")
    if not isinstance(outer, dict):
        return "unknown", None
    inner = outer.get("order")
    if not isinstance(inner, dict):
        return "unknown", None
    cloid = inner.get("cloid")
    if is_bot_cloid(cloid):
        return "bot", str(cloid)
    return "external", None


class CloidFactory:
    def __init__(self) -> None:
        self.run = int.from_bytes(os.urandom(4), "big")
        self.seq = 0

    def next(self) -> str:
        self.seq += 1
        raw = (
            (BOT_CLOID_MAGIC << 96)
            | (self.run << 64)
            | (int(time.time() * 1000) & 0xFFFFFFFFFFFF) << 16
            | (self.seq & 0xFFFF)
        )
        return f"{raw:#034x}"


class Broker(abc.ABC):
    def __init__(self, cfg: Config, meta: MarketMeta, fees: FeeModel) -> None:
        self.cfg = cfg
        self.meta = meta
        self.fees = fees
        self.cloids = CloidFactory()
        self.orders_by_role: dict[str, TrackedOrder] = {}
        self._fills: list[Fill] = []
        self.account = AccountState()
        # Official-state drift is kept as counts only.  The engine can fail
        # closed without persisting or exposing exchange identifiers.
        self.official_untracked_bot_orders = 0
        self.official_foreign_orders = 0
        self.official_leverage_type: str | None = None
        self.official_leverage_value: int | None = None
        self.requotes_modified = 0
        self.requotes_replaced = 0
        # Order lifecycle events for the local replay ledger.  The engine
        # drains these into state/*_orders.jsonl alongside fills/markouts.
        self._order_events: list[dict] = []
        # Grid mode can provide a synchronous, fsync-backed intent journal.
        # A failed journal write must happen BEFORE any non-idempotent send.
        # Ordinary cancellation/reconciliation logging does not use this hook.
        self.order_intent_sink: Callable[[dict], None] | None = None

    def _note_order_event(
        self, event: str, *, durable_intent: bool = False, **fields: object
    ) -> dict:
        row: dict = {"time_ms": int(time.time() * 1000), "event": event}
        row.update(fields)
        if durable_intent and self.order_intent_sink is not None:
            self.order_intent_sink(dict(row))
        self._order_events.append(row)
        # Bounded memory if the engine stalls; the ledger is best-effort.
        if len(self._order_events) > 10_000:
            del self._order_events[:5_000]
        return row

    def drain_order_events(self) -> list[dict]:
        events, self._order_events = self._order_events, []
        return events

    @abc.abstractmethod
    async def start(self) -> None: ...

    @abc.abstractmethod
    async def place(self, intents: list[OrderIntent], book: OrderBook) -> None: ...

    @abc.abstractmethod
    async def cancel_roles(self, roles: Iterable[str]) -> None: ...

    @abc.abstractmethod
    async def sync(self, book: OrderBook) -> None: ...

    @abc.abstractmethod
    async def flatten(self, book: OrderBook, slippage_bps: float) -> None: ...

    async def cancel_all(self) -> None:
        await self.cancel_roles(list(self.orders_by_role))

    async def modify_orders(
        self, pairs: list[tuple[str, OrderIntent]], book: OrderBook
    ) -> None:
        """Reprice tracked roles.  Default: plain cancel + place.

        LiveBroker overrides this with atomic batchModify.  The fallback keeps
        PaperBroker and any degraded path semantically identical: the role is
        released first, then re-placed at the new price.
        """
        roles = [role for role, _ in pairs]
        self.requotes_modified += len(roles)
        await self.cancel_roles(roles)
        replacements = [
            intent for role, intent in pairs if role not in self.orders_by_role
        ]
        if replacements:
            await self.place(replacements, book)

    def ws_healthy(self) -> bool:
        return False

    def drain_fills(self) -> list[Fill]:
        fills, self._fills = self._fills, []
        return fills


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


class LiveBroker(Broker):
    def __init__(
        self,
        cfg: Config,
        meta: MarketMeta,
        fees: FeeModel,
        market: MarketClient,
        private_key: str,
        account_address: str,
    ) -> None:
        super().__init__(cfg, meta, fees)
        self.market = market
        self.private_key = private_key
        self.address = account_address.lower()
        self.exchange = None
        self._lock = asyncio.Lock()
        self._last_fill_ms = int(time.time() * 1000) - 10_000
        self._seen_fills: set[str] = set()
        self._seen_fill_order: deque[str] = deque()
        self._oid_to_role: dict[int, str] = {}
        self._retired_orders_by_oid: dict[int, tuple[TrackedOrder, float]] = {}
        # A non-idempotent IOC whose send result cannot be proven is retained
        # by cloid.  Every later IOC reconciles these identities first and is
        # blocked while even one remains ambiguous.
        self._pending_ioc_orders: dict[str, TrackedOrder] = {}
        # Order existence is monotonic: once either the SDK or orderStatus has
        # proven that an IOC reached the exchange, a later unknownOid must not
        # be reinterpreted as "never landed" and release a blind retry.
        self._proven_ioc_cloids: set[str] = set()
        # A place/modify can reach the exchange even when the client only sees
        # a timeout plus transient unknownOid lookups.  Keep that generated
        # cloid attached to its strategy role until a positive official view
        # binds the numeric oid; absence is never proof of non-submission.
        self._identity_pending_cloids: set[str] = set()
        # Link an ambiguous conditional modify to the exact predecessor.  A
        # complete predecessor fill proves the replacement was not installed.
        self._uncertain_modify_predecessors: dict[str, TrackedOrder] = {}
        # A fill page can lead frontendOpenOrders within the same sync cycle.
        # Keep that contradictory, already-known order separate from genuine
        # bot orphans while replacement is paused for a bounded interval.
        self._retired_open_since: dict[str, float] = {}
        self._visible_retired_orders: dict[str, TrackedOrder] = {}
        self._expired_retired_order_cloids: set[str] = set()
        self._missing_since: dict[str, float] = {}
        self._recent_fill_fallback_at: dict[str, float] = {}
        self._deep_fill_fallback_done: set[str] = set()
        self._cancel_pending_since: dict[str, float] = {}
        self._cancel_last_submit_at: dict[str, float] = {}
        self._cancel_submit_attempts: dict[str, int] = {}
        self._ro_clip_probe_at: dict[str, float] = {}
        self._session_realized = 0.0
        self.account_abstraction = "default"
        self._last_fill_sync = 0.0
        self._last_full_fill_sync = 0.0
        self._last_account_sync = 0.0
        self._last_rest_sweep = 0.0
        # --- websocket push-state caches -------------------------------
        # These mirror official REST views.  They are cleared whenever the
        # shared websocket reconnects (session bump) and are only trusted
        # while the connection is visibly alive; REST remains the authority
        # on its own low-frequency clock.
        self._ws_enabled = bool(cfg.runtime.use_ws_user_feeds)
        self._ws_session = 0
        self._ws_order_status: dict[str, dict] = {}
        self._ws_open_orders: list | None = None
        self._ws_clearinghouse: dict | None = None
        self._ws_spot: dict | None = None
        self._ws_active_asset: dict | None = None
        self._last_ws_fill_ms = 0
        self._last_applied_fill_ms = 0
        self._position_snapshot_ms = 0
        # --- address-based action budget -------------------------------
        self.action_budget = ActionBudget(
            soft_remaining=float(cfg.runtime.budget_soft_remaining),
            hard_remaining=float(cfg.runtime.budget_hard_remaining),
        )
        self._rate_limit_address = self.address
        self._budget_next_poll = 0.0
        self._reserve_cooldown_until = 0.0
        self._reserved_usd_today = 0.0
        self._reserved_usd_day = ""
        self._action_rate_limited_until = 0.0

    # ------------------------------------------------------------------
    # Websocket push-state ingestion
    # ------------------------------------------------------------------

    def ingest_ws_event(self, channel: str, data: object) -> None:
        """Apply one push message from the shared market websocket.

        Runs inside the market feed task: it must never raise, and it only
        updates caches/local estimates.  Every decision that places or
        cancels orders still goes through the sync/reconcile paths, which
        fall back to REST whenever these caches are stale.
        """
        try:
            self._ingest_ws_event(channel, data)
        except Exception:
            self._ws_open_orders = None
            self._last_rest_sweep = 0.0
            self._last_account_sync = 0.0
            log.exception("忽略无法解析的 WS 推送: channel=%s", channel)

    def _ingest_ws_event(self, channel: str, data: object) -> None:
        if channel == "ws_reset":
            if isinstance(data, dict) and data.get("session") is not None:
                self._ws_session = int(data["session"])
            self._ws_open_orders = None
            self._ws_clearinghouse = None
            self._ws_spot = None
            self._ws_active_asset = None
            return
        if not self._ws_enabled:
            return
        if channel == "orderUpdates" and isinstance(data, list):
            now = time.monotonic()
            for row in data:
                if not isinstance(row, dict):
                    continue
                order = row.get("order") or {}
                if order.get("coin") != self.meta.coin:
                    continue
                cloid = str(order.get("cloid") or "")
                if not cloid:
                    continue
                pending_ioc = self._pending_ioc_by_identity(
                    cloid=cloid, oid=order.get("oid")
                )
                if pending_ioc is not None:
                    self._mark_ioc_proven(pending_ioc)
                    try:
                        pending_oid = int(order.get("oid") or 0)
                    except (TypeError, ValueError):
                        pending_oid = 0
                    if pending_oid:
                        self._register_ioc_identity(pending_ioc, pending_oid)
                self._bind_active_order_identity(
                    cloid=cloid, oid=order.get("oid")
                )
                # Synthesize the REST orderStatus payload shape so downstream
                # reconciliation code paths stay byte-for-byte identical.
                self._ws_order_status[cloid] = {
                    "mono": now,
                    "payload": {
                        "status": "order",
                        "order": {
                            "status": str(row.get("status") or ""),
                            "statusTimestamp": row.get("statusTimestamp"),
                            "order": dict(order),
                        },
                    },
                }
            if len(self._ws_order_status) > 4000:
                oldest = sorted(
                    self._ws_order_status.items(), key=lambda kv: kv[1]["mono"]
                )
                self._ws_order_status = dict(oldest[-2000:])
            return
        if channel == "userFills" and isinstance(data, dict):
            rows = data.get("fills") or []
            if not isinstance(rows, list):
                return
            for raw in sorted(
                (r for r in rows if isinstance(r, dict)),
                key=lambda x: int(x.get("time") or 0),
            ):
                row_time = self._ingest_fill_row(raw)
                self._last_ws_fill_ms = max(self._last_ws_fill_ms, row_time)
            # The REST cursor (_last_fill_ms) is intentionally NOT advanced
            # here: the periodic authoritative sweep re-reads the same window
            # and the tid dedupe set absorbs the overlap.  A silently dropped
            # WS row therefore cannot create a permanent gap.
            return
        if channel == "clearinghouseState" and isinstance(data, dict):
            if str(data.get("dex") or "") != self.cfg.market.dex:
                return
            inner = data.get("clearinghouseState")
            if isinstance(inner, dict):
                self._ws_clearinghouse = inner
                self._apply_ws_account()
            return
        if channel == "openOrders" and isinstance(data, dict):
            if str(data.get("dex") or "") != self.cfg.market.dex:
                return
            rows = data.get("orders")
            if isinstance(rows, list):
                self._ws_open_orders = rows
                for raw in rows:
                    if not isinstance(raw, dict) or raw.get("coin") != self.meta.coin:
                        continue
                    pending_ioc = self._pending_ioc_by_identity(
                        cloid=str(raw.get("cloid") or ""), oid=raw.get("oid")
                    )
                    if pending_ioc is not None:
                        self._mark_ioc_proven(pending_ioc)
                        try:
                            pending_oid = int(raw.get("oid") or 0)
                        except (TypeError, ValueError):
                            pending_oid = 0
                        if pending_oid:
                            self._register_ioc_identity(pending_ioc, pending_oid)
                    self._bind_active_order_identity(
                        cloid=str(raw.get("cloid") or ""), oid=raw.get("oid")
                    )
            return
        if channel == "activeAssetData" and isinstance(data, dict):
            if data.get("coin") == self.meta.coin:
                self._ws_active_asset = data
                self._apply_ws_account()
            return
        if channel == "spotState" and isinstance(data, dict):
            if isinstance(data.get("balances"), list):
                self._ws_spot = data
                self._apply_ws_account()
            return

    def _ws_connection_alive(self) -> bool:
        if not self._ws_enabled:
            return False
        ws_session = getattr(self.market, "ws_session", 0)
        alive_check = getattr(self.market, "ws_alive_within", None)
        return (
            self._ws_session == ws_session
            and callable(alive_check)
            and bool(alive_check(WS_STALE_FALLBACK_SEC))
        )

    def _ws_open_view(self) -> tuple[set[str], set[int]] | None:
        """Same-market open-order identity sets from the push snapshot."""
        if self._ws_open_orders is None or not self._ws_connection_alive():
            return None
        cloids: set[str] = set()
        oids: set[int] = set()
        for raw in self._ws_open_orders:
            if not isinstance(raw, dict) or raw.get("coin") != self.meta.coin:
                continue
            cloid = raw.get("cloid")
            if cloid:
                cloids.add(str(cloid))
            try:
                oids.add(int(raw.get("oid")))
            except (TypeError, ValueError):
                pass
        return cloids, oids

    def ws_healthy(self) -> bool:
        return self._ws_open_view() is not None

    def _ws_terminal_payload(self, cloid: str) -> dict | None:
        """Immutable terminal status from push events (safe at any age).

        Cloids are unique per order in this bot, and filled/canceled are
        absorbing states, so a terminal push event can be trusted even after
        the connection died.  Non-terminal events are never returned.
        """
        entry = self._ws_order_status.get(cloid)
        if entry is None:
            return None
        payload = entry["payload"]
        state = _order_state(payload)
        if state == "filled" or state in CANCELED_ORDER_STATES:
            return payload
        return None

    async def _order_status_view(self, cloid: str) -> dict | None:
        payload = self._ws_terminal_payload(cloid)
        if payload is not None:
            return payload
        return await self.market.order_status(self.address, cloid)

    def _apply_ws_account(self) -> None:
        """Refresh account state from push caches when a complete set exists.

        The REST account sync keeps running on its own clock as the
        authority; this only shortens the latency between authoritative
        snapshots (position caps react to fills within one push instead of
        one poll interval).
        """
        inner = self._ws_clearinghouse
        if inner is None:
            return
        try:
            payload_time = int(inner.get("time") or 0)
        except (TypeError, ValueError):
            payload_time = 0
        # A clearinghouse snapshot older than the newest locally applied fill
        # would resurrect a stale position estimate; wait for the next push.
        if payload_time and payload_time < max(self._last_applied_fill_ms, self._position_snapshot_ms):
            return
        try:
            if self.account_abstraction == "unifiedAccount":
                if self._ws_spot is None or self._ws_active_asset is None:
                    return
                self._apply_account_payload(
                    inner, self._ws_spot, self._ws_active_asset
                )
            else:
                self._apply_account_payload(inner, {}, self._ws_active_asset or {})
        except Exception:
            self._ws_open_orders = None
            self._last_rest_sweep = 0.0
            self._last_account_sync = 0.0
            log.exception("WS 账户推送解析失败；等待 REST 权威刷新")

    def _retire_order(self, order: TrackedOrder) -> None:
        """Keep terminal order identity long enough for a delayed fill row.

        HIP-3 orderStatus/open-order views can become terminal several minutes
        before userFillsByTime exposes the final row.  Releasing the strategy
        role is safe after a confirmed zero-fill cancellation, but discarding
        its OID/Cloid identity would make a later official fill unrecordable.
        """
        if order.oid is None:
            return
        self._oid_to_role[order.oid] = order.role
        self._retired_orders_by_oid[order.oid] = (
            order,
            time.monotonic() + RETIRED_ORDER_FILL_GRACE_SEC,
        )

    def _retire_active_order(self, order: TrackedOrder) -> None:
        """Release one reconciled role while preserving late-fill identity."""
        if self.orders_by_role.get(order.role) is not order:
            return
        self.orders_by_role.pop(order.role, None)
        self._missing_since.pop(order.cloid, None)
        self._recent_fill_fallback_at.pop(order.cloid, None)
        self._deep_fill_fallback_done.discard(order.cloid)
        self._clear_cancel_tracking(order.cloid)
        self._ro_clip_probe_at.pop(order.cloid, None)
        self._identity_pending_cloids.discard(order.cloid)
        self._uncertain_modify_predecessors.pop(order.cloid, None)
        self._retire_order(order)

    def _note_action_rate_limit(self) -> None:
        """Pause quote mutations briefly after an explicit HTTP 429."""

        self._action_rate_limited_until = max(
            self._action_rate_limited_until, time.monotonic() + 10.0
        )

    @property
    def action_rate_limited(self) -> bool:
        return time.monotonic() < self._action_rate_limited_until

    def _clear_cancel_tracking(self, cloid: str) -> None:
        """Drop every retry guard after an order reaches a reconciled end."""
        self._cancel_pending_since.pop(cloid, None)
        self._cancel_last_submit_at.pop(cloid, None)
        self._cancel_submit_attempts.pop(cloid, None)

    async def _reduce_only_clip_released(
        self, order: TrackedOrder, elapsed: float, *, force: bool = False
    ) -> bool:
        """Release a reduce-only order whose remainder the exchange clipped.

        A reduce-only order may partially fill until the position reaches
        zero, after which the exchange cancels its remainder. A terminal
        ``filled`` view can still make ``origSz - sz`` overstate the executed
        quantity. Waiting for the unfilled remainder would then block
        reconciliation unnecessarily.

        The clearinghouse is the authority here: a reduce-only order cannot
        fill beyond the position, so once nothing is left to reduce in the
        order's direction, no further fill for this order can exist and
        releasing the role cannot double an order. Any late userFills rows
        still ingest through the retired-OID mapping for ledger accuracy."""
        if not order.reduce_only:
            return False
        # Give the ordinary 1-2 s fill propagation a moment before spending a
        # clearinghouse request, then re-probe at most every 20 s per order.
        # ``force`` is the last check before a hard stop and must not be lost
        # to the throttle.
        now = time.monotonic()
        if not force:
            if elapsed < 5.0:
                return False
            if now - self._ro_clip_probe_at.get(order.cloid, 0.0) < 20.0:
                return False
        self._ro_clip_probe_at[order.cloid] = now
        await self._sync_position_only()
        tolerance = self.meta.size_step / 2
        position = self.account.position_qty
        nothing_left_to_reduce = (
            position >= -tolerance if order.is_buy else position <= tolerance
        )
        if not nothing_left_to_reduce:
            return False
        log.warning(
            "减仓订单 %s 官方仓位已无该方向可减余量（本地明细 %.8f/%.8f）；"
            "剩余部分视为被交易所削减，释放角色",
            order.role,
            order.filled_qty,
            order.qty,
        )
        self._retire_active_order(order)
        return True

    def _reduce_only_fill_is_fully_reconciled(
        self, order: TrackedOrder, reported_fill: float
    ) -> bool:
        """Accept an exchange-clipped reduce-only terminal fill safely.

        A reduce-only order can be requested for more than the remaining
        position.  Once that position reaches zero, the exchange may report
        the order as ``filled`` with a smaller cumulative fill than the local
        requested quantity.  Waiting for the clipped remainder would block
        forever.  Release it only when the authoritative cumulative amount is
        positive, meaningfully below the request, and every reported unit is
        already represented by local userFills rows.
        """
        tolerance = self.meta.size_step / 2
        return (
            order.reduce_only
            and reported_fill > tolerance
            and reported_fill + tolerance < order.qty
            and abs(order.filled_qty - reported_fill) <= tolerance
        )

    def _prune_retired_orders(self) -> None:
        now = time.monotonic()
        for oid, (order, expires_at) in list(self._retired_orders_by_oid.items()):
            if expires_at > now:
                continue
            self._retired_orders_by_oid.pop(oid, None)
            self._retired_open_since.pop(order.cloid, None)
            self._expired_retired_order_cloids.discard(order.cloid)
            if self._oid_to_role.get(oid) == order.role:
                self._oid_to_role.pop(oid, None)

    async def _sync_position_only(self) -> None:
        """Refresh exposure after an ambiguous canceled-order size change.

        orderStatus does not document canceled-order ``sz`` as a cumulative
        fill field.  Clearinghouse position is the low-weight authoritative
        guard against reposting too much inventory while a late userFills row
        is still recoverable through the retired OID mapping.
        """
        state = await self.market.account_state(self.address)
        positions = _validated_positions(state)
        snapshot_ms = self._check_position_snapshot_time(state)
        position = next(
            (
                (item.get("position") or {})
                for item in positions
                if (item.get("position") or {}).get("coin") == self.meta.coin
            ),
            None,
        )
        if position:
            self._update_official_leverage(position)
            self.account.position_qty = float(position.get("szi") or 0.0)
            self.account.entry_px = float(position.get("entryPx") or 0.0)
            self.account.unrealized_pnl = float(position.get("unrealizedPnl") or 0.0)
            liquidation_px = position.get("liquidationPx")
            self.account.liquidation_px = (
                float(liquidation_px)
                if liquidation_px not in (None, "")
                else None
            )
        else:
            self.account.position_qty = 0.0
            self.account.entry_px = 0.0
            self.account.unrealized_pnl = 0.0
            self.account.liquidation_px = None
        if snapshot_ms:
            self._position_snapshot_ms = snapshot_ms

    def _check_position_snapshot_time(self, state: dict) -> int:
        timestamp = state.get("time")
        if isinstance(timestamp, bool) or not isinstance(timestamp, int) or timestamp <= 0:
            raise RuntimeError("官方仓位快照时间缺失或无效；不能判断成交是否已计入")
        if timestamp < max(self._last_applied_fill_ms, self._position_snapshot_ms):
            raise PendingFillReconciliation("官方仓位快照早于已确认成交；保留最新仓位等待对账")
        return timestamp

    def _update_official_leverage(self, data: dict | None) -> None:
        leverage = (data or {}).get("leverage") or {}
        kind = str(leverage.get("type") or "")
        try:
            value = int(leverage.get("value") or 0)
        except (TypeError, ValueError):
            value = 0
        self.official_leverage_type = kind or None
        self.official_leverage_value = value or None

    def _bind_active_order_identity(
        self, *, cloid: str = "", oid: object = None
    ) -> TrackedOrder | None:
        """Bind a positive official identity to one active strategy role.

        Matching by either cloid or oid is allowed, but two views that point to
        different active roles are a hard identity conflict.  This is also the
        adoption path for fail-closed place/modify placeholders whose oid was
        not visible when the signed action returned.
        """

        normalized_cloid = str(cloid or "").lower()
        try:
            normalized_oid = int(oid or 0)
        except (TypeError, ValueError):
            normalized_oid = 0

        by_cloid = next(
            (
                order
                for order in self.orders_by_role.values()
                if normalized_cloid and order.cloid.lower() == normalized_cloid
            ),
            None,
        )
        by_oid = next(
            (
                order
                for order in self.orders_by_role.values()
                if normalized_oid and order.oid == normalized_oid
            ),
            None,
        )
        if by_cloid is not None and by_oid is not None and by_cloid is not by_oid:
            raise RuntimeError("官方订单 cloid/oid 指向不同策略角色；已停机防止重复挂单")
        order = by_cloid or by_oid
        if order is None:
            return None
        if normalized_cloid and order.cloid.lower() != normalized_cloid:
            raise RuntimeError("官方订单 oid 对应了意外 cloid；已停机防止身份串单")
        if normalized_oid:
            if order.oid is not None and order.oid != normalized_oid:
                raise RuntimeError("官方订单 cloid 对应了意外 oid；已停机防止身份串单")
            mapped_role = self._oid_to_role.get(normalized_oid)
            if mapped_role is not None and mapped_role != order.role:
                raise RuntimeError("官方订单 oid 已归属于其他策略角色；已停机防止身份串单")
            was_identity_pending = order.cloid in self._identity_pending_cloids
            order.oid = normalized_oid
            self._oid_to_role[normalized_oid] = order.role
            self._identity_pending_cloids.discard(order.cloid)
            self._uncertain_modify_predecessors.pop(order.cloid, None)
            if was_identity_pending:
                # Persist the positive adoption separately from the original
                # action row.  It is identity metadata, not another address
                # action, and lets stopped recovery distinguish a resolved
                # placeholder from one that vanished with the process.
                self._note_order_event(
                    "order_identity",
                    role=order.role,
                    cloid=order.cloid,
                    oid=normalized_oid,
                    is_buy=order.is_buy,
                    px=order.price,
                    qty=order.qty,
                    reduce_only=order.reduce_only,
                    tif=order.tif,
                )
        return order

    def _classify_official_orders(self, open_raw: list) -> list[str]:
        """Record same-market order drift and return cancelable bot orphans.

        Orders owned by the current in-memory mapping are expected.  A bot
        Cloid from another run is safe to cancel, while an order without this
        bot's Cloid namespace is never touched automatically and instead makes
        the engine fail closed.
        """
        tracked_cloids = {order.cloid for order in self.orders_by_role.values()}
        tracked_oids = {
            int(order.oid)
            for order in self.orders_by_role.values()
            if order.oid is not None
        }
        retired_by_cloid = {
            order.cloid: order
            for order, _expires_at in self._retired_orders_by_oid.values()
        }
        bot_orphans: list[str] = []
        visible_retired: dict[str, TrackedOrder] = {}
        expired_retired: set[str] = set()
        foreign = 0
        now = time.monotonic()
        for raw in open_raw:
            if raw.get("coin") != self.meta.coin:
                continue
            cloid = str(raw.get("cloid") or "")
            try:
                oid = int(raw.get("oid"))
            except (TypeError, ValueError):
                oid = None
            pending_ioc = self._pending_ioc_by_identity(cloid=cloid, oid=oid)
            if pending_ioc is not None:
                self._mark_ioc_proven(pending_ioc)
                if oid is not None:
                    self._register_ioc_identity(pending_ioc, oid)
                continue
            active_order = self._bind_active_order_identity(cloid=cloid, oid=oid)
            if active_order is not None:
                continue
            if cloid in tracked_cloids or (oid is not None and oid in tracked_oids):
                continue
            retired_order = None
            if oid is not None:
                retired_entry = self._retired_orders_by_oid.get(oid)
                if retired_entry is not None and cloid in {"", retired_entry[0].cloid}:
                    retired_order = retired_entry[0]
            if retired_order is None and cloid:
                cloid_match = retired_by_cloid.get(cloid)
                if cloid_match is not None and (
                    oid is None or cloid_match.oid is None or oid == cloid_match.oid
                ):
                    retired_order = cloid_match
            if retired_order is not None:
                first_seen = self._retired_open_since.get(retired_order.cloid)
                if (
                    first_seen is None
                    or now - first_seen < ORDER_RECONCILIATION_GRACE_SEC
                ):
                    visible_retired[retired_order.cloid] = retired_order
                    continue
                expired_retired.add(retired_order.cloid)
            if is_bot_cloid(cloid):
                bot_orphans.append(cloid)
            else:
                foreign += 1
        self._visible_retired_orders = visible_retired
        self._expired_retired_order_cloids = expired_retired
        self.official_untracked_bot_orders = len(bot_orphans)
        self.official_foreign_orders = foreign
        return bot_orphans

    def _retired_open_pending_message(self) -> str:
        count = len(self._retired_open_since)
        oldest = min(self._retired_open_since.values(), default=time.monotonic())
        elapsed = max(time.monotonic() - oldest, 0.0)
        return (
            f"{count} 笔已完成订单仍出现在开放委托快照；"
            f"已暂停替换（{elapsed:.0f}/{ORDER_RECONCILIATION_GRACE_SEC:.0f} 秒）"
        )

    async def _assert_recent_external_activity_quiet(self) -> None:
        """Report recent outside fills without turning history into a lock.

        Current foreign open orders are checked separately and still block
        every signed mutation.  Historical fills are not active orders; the
        authoritative startup account snapshot adopts their resulting
        position, so forcing a 15-minute manual wait only makes the green
        start button unreliable without reducing present risk.
        """
        start_ms = int(
            (time.time() - STARTUP_EXTERNAL_ACTIVITY_QUIET_SEC) * 1000
        )
        fills = await self.market.fills_since(self.address, start_ms)
        accepted = {self.meta.coin, self.meta.symbol}
        same_market = [row for row in fills if row.get("coin") in accepted]
        oids = {
            int(row.get("oid") or 0)
            for row in same_market
            if int(row.get("oid") or 0) > 0
        }
        invalid_oid_count = sum(
            1 for row in same_market if int(row.get("oid") or 0) <= 0
        )
        external_orders = 0
        unknown_orders = invalid_oid_count
        for oid in sorted(oids):
            try:
                status = await self.market.order_status(self.address, oid)  # type: ignore[arg-type]
            except Exception:
                status = None
            ownership, _ = order_ownership(status)
            if ownership == "external":
                external_orders += 1
            elif ownership == "unknown":
                unknown_orders += 1
        if unknown_orders:
            log.warning(
                "最近 15 分钟有 %d 个同市场历史成交暂无法确认归属；"
                "将按官方仓位接管，当前开放委托仍单独严格核验",
                unknown_orders,
            )
        if external_orders:
            log.warning(
                "最近 15 分钟检测到 %d 个外部同市场历史成交；"
                "不阻止显式启动，当前开放委托仍单独严格核验",
                external_orders,
            )

    async def _assert_no_foreign_open_orders_before_mutation(self) -> None:
        """Fail closed on foreign same-market orders before any signed write.

        A quiet recent-fill window does not prove that another client has no
        resting order.  In particular, leverage/margin changes affect the same
        market account, so the authoritative open-order view must be classified
        before the Exchange signer is created or `_ensure_target_leverage()` can
        submit anything.  Bot-namespace orphans remain eligible for the normal
        startup cancellation path; foreign orders are never touched.
        """

        open_orders = await self.market.open_orders(self.address)
        self._classify_official_orders(open_orders)
        if self.official_foreign_orders:
            raise RuntimeError(
                "开放委托中检测到 "
                f"{self.official_foreign_orders} 笔外部同市场订单；"
                "为避免在外部订单存在时改变杠杆/保证金，拒绝启动"
            )

    async def start(self) -> None:
        try:
            from eth_account import Account
            from hyperliquid.exchange import Exchange
        except ImportError as exc:
            raise RuntimeError(
                "实盘需要安装 requirements-live.txt（官方 hyperliquid-python-sdk）"
            ) from exc
        wallet = Account.from_key(self.private_key)
        if wallet.address.lower() == self.address:
            account_role, account_abstraction = await asyncio.gather(
                self.market.user_role(self.address),
                self.market.user_abstraction(self.address),
            )
            signer_role = account_role
        else:
            signer_role, account_role, account_abstraction = await asyncio.gather(
                self.market.user_role(wallet.address),
                self.market.user_role(self.address),
                self.market.user_abstraction(self.address),
            )
        self.account_abstraction = str(account_abstraction or "default")
        if self.account_abstraction == "portfolioMargin":
            raise RuntimeError(
                "当前为 portfolioMargin；本版本尚未实现跨资产组合保证金精确核算，拒绝实盘"
            )
        target_role = account_role.get("role")
        if target_role == "user":
            target_master = self.address
        elif target_role == "subAccount":
            target_master = str(
                (account_role.get("data") or {}).get("master") or ""
            ).lower()
        else:
            raise RuntimeError(
                f"HL_ACCOUNT_ADDRESS 不是主账户或子账户: {target_role or 'missing'}"
            )

        signer_kind = signer_role.get("role")
        signer_address = wallet.address.lower()
        if signer_kind == "user":
            if signer_address != target_master:
                raise RuntimeError(
                    "主钱包私钥与目标主账户/子账户所属主账户不匹配"
                )
            signer_label = "main-wallet"
        elif signer_kind == "agent":
            master = str(
                (signer_role.get("data") or {}).get("user") or ""
            ).lower()
            if not master or master != target_master:
                raise RuntimeError("API Wallet 未被这个主账户/子账户所属主账户授权")
            signer_label = "api-wallet"
        else:
            raise RuntimeError(
                f"签名钱包不是可用的主钱包或 API Wallet: {signer_kind or 'missing'}"
            )
        # Official address limits treat subaccounts as separate users. Poll
        # the actual trading account, not its signing/authorization master.
        self._rate_limit_address = self.address
        await self._assert_recent_external_activity_quiet()
        await self._assert_no_foreign_open_orders_before_mutation()
        # The official constructor fetches spotMeta, perpDexs and meta with
        # synchronous requests. Keep those reads off the event-loop thread;
        # signed actions still use the existing async/process mutex below.
        self.exchange = await asyncio.to_thread(
            Exchange,
            wallet,
            base_url=self.cfg.market.api_url,
            vault_address=self.address if target_role == "subAccount" else None,
            account_address=self.address,
            perp_dexs=[self.cfg.market.dex],
            timeout=10.0,
        )
        self._process_mutex_name = (
            "Local\\EntropyTraderExchange-"
            + hashlib.sha256(self.address.lower().encode("ascii")).hexdigest()[:16]
        )
        await self._ensure_target_leverage()
        log.info(
            "实盘签名器已加载: type=%s signer=%s account=%s abstraction=%s",
            signer_label,
            wallet.address[:6] + "..." + wallet.address[-4:],
            self.address[:6] + "..." + self.address[-4:],
            self.account_abstraction,
        )

    async def _ensure_target_leverage(self) -> None:
        target = self.cfg.market.target_leverage
        if target is None:
            return
        if target > self.meta.max_leverage:
            raise RuntimeError(
                f"目标杠杆 {target}x 超过 {self.meta.coin} 上限 "
                f"{self.meta.max_leverage}x"
            )

        async def current() -> tuple[str, int, dict]:
            data = await self.market.active_asset_data(self.address)
            self._update_official_leverage(data)
            leverage = data.get("leverage") or {}
            value = self.official_leverage_value or 0
            return str(leverage.get("type") or ""), value, data

        kind, value, _ = await current()
        if kind == "isolated" and value == target:
            log.info("逐仓杠杆已核验: %s %dx", self.meta.coin, target)
            return
        assert self.exchange is not None

        async def submit_leverage() -> dict:
            try:
                result = await self._sdk(
                    self.exchange.update_leverage,
                    target,
                    self.meta.coin,
                    False,
                )
            except Exception as exc:
                # Reconcile official state once; never blindly submit it again.
                await asyncio.sleep(0.5)
                now_kind, now_value, _ = await current()
                if now_kind == "isolated" and now_value == target:
                    log.info(
                        "杠杆提交响应不确定，但官方状态已确认: %s %dx",
                        self.meta.coin,
                        target,
                    )
                    return {"status": "ok", "reconciled": True}
                raise RuntimeError("杠杆更新结果不确定，已停止且不重试") from exc
            return result if isinstance(result, dict) else {"status": "invalid", "response": result}

        async def add_required_margin() -> None:
            state, active = await asyncio.gather(
                self.market.account_state(self.address),
                self.market.active_asset_data(self.address),
            )
            position = next(
                (
                    (item.get("position") or {})
                    for item in state.get("assetPositions") or []
                    if (item.get("position") or {}).get("coin") == self.meta.coin
                ),
                None,
            )
            if not position:
                raise RuntimeError("杠杆下降需要追加保证金，但官方未返回目标仓位")
            position_value = abs(float(position.get("positionValue") or 0.0))
            margin_used = float(position.get("marginUsed") or 0.0)
            required = position_value / target
            amount = round(max(required - margin_used, 0.0) + 1.0, 6)
            available = [float(x) for x in (active.get("availableToTrade") or [])]
            free = min(available) if len(available) == 2 else 0.0
            if amount <= 0 or amount > self.cfg.risk.capital_usd * 0.2:
                raise RuntimeError(f"自动追加逐仓保证金金额异常: ${amount:.2f}")
            if free - amount < self.cfg.risk.min_free_collateral_usd:
                raise RuntimeError("追加逐仓保证金后将低于可用 USDC 保留线")
            before_raw = float(((position.get("leverage") or {}).get("rawUsd")) or 0.0)
            try:
                result = await self._sdk(
                    self.exchange.update_isolated_margin,
                    amount,
                    self.meta.coin,
                )
            except Exception as exc:
                await asyncio.sleep(0.5)
                after_state = await self.market.account_state(self.address)
                after_position = next(
                    (
                        (item.get("position") or {})
                        for item in after_state.get("assetPositions") or []
                        if (item.get("position") or {}).get("coin") == self.meta.coin
                    ),
                    {},
                )
                after_raw = float(
                    ((after_position.get("leverage") or {}).get("rawUsd")) or 0.0
                )
                if after_raw >= before_raw + amount - 0.02:
                    log.info(
                        "追加保证金响应不确定，但官方状态已确认: %s $%.2f",
                        self.meta.coin,
                        amount,
                    )
                    return
                raise RuntimeError("追加逐仓保证金结果不确定，已停止且不重试") from exc
            if not isinstance(result, dict) or result.get("status") != "ok":
                raise RuntimeError(f"追加逐仓保证金失败: {str(result)[:240]}")
            log.info("已为 %s 追加逐仓保证金 $%.2f", self.meta.coin, amount)
            await asyncio.sleep(0.5)

        response = await submit_leverage()
        error_text = str(response.get("response") or "")
        if response.get("status") != "ok" and "sufficient margin" in error_text.lower():
            await add_required_margin()
            response = await submit_leverage()
        if response.get("status") != "ok":
            raise RuntimeError(f"杠杆更新失败: {str(response)[:240]}")
        kind, value, _ = await current()
        if kind != "isolated" or value != target:
            raise RuntimeError(
                f"杠杆写入后官方状态不一致: {kind or 'unknown'} {value}x"
            )
        log.info("逐仓杠杆已设置并核验: %s %dx", self.meta.coin, target)

    def _sdk_with_process_lock(self, fn, *args):
        if os.name != "nt":
            return fn(*args)
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        create_mutex = kernel32.CreateMutexW
        create_mutex.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_wchar_p]
        create_mutex.restype = ctypes.c_void_p
        wait_one = kernel32.WaitForSingleObject
        wait_one.argtypes = [ctypes.c_void_p, ctypes.c_ulong]
        wait_one.restype = ctypes.c_ulong
        release_mutex = kernel32.ReleaseMutex
        release_mutex.argtypes = [ctypes.c_void_p]
        release_mutex.restype = ctypes.c_int
        close_handle = kernel32.CloseHandle
        close_handle.argtypes = [ctypes.c_void_p]
        close_handle.restype = ctypes.c_int
        handle = create_mutex(None, 0, self._process_mutex_name)
        if not handle:
            raise RuntimeError("无法创建交易签名进程锁")
        try:
            result = wait_one(handle, 30_000)
            if result not in {0x00000000, 0x00000080}:
                raise RuntimeError("等待交易签名进程锁超时")
            try:
                value = fn(*args)
                # Hyperliquid actions use millisecond nonces. Keep consecutive
                # processes from signing two actions in the same millisecond.
                time.sleep(0.003)
                return value
            finally:
                release_mutex(handle)
        finally:
            close_handle(handle)

    async def _sdk(self, fn, *args):
        async with self._lock:
            return await asyncio.to_thread(self._sdk_with_process_lock, fn, *args)

    async def cancel_bot_orphans(self) -> int:
        open_orders = await self.market.open_orders(self.address)
        cloids = self._classify_official_orders(open_orders)
        if not cloids:
            return 0
        for cloid in cloids:
            if cloid in self._cancel_pending_since:
                continue
            self._note_order_event(
                "cancel",
                role="risk:orphan-cancel",
                cloid=cloid,
                reason="startup_bot_orphan",
            )
        await self._cancel_cloids(cloids)
        # Orphans have no live strategy role to reserve.  Once the official
        # open-order view no longer contains them, the bounded cancel guard
        # can be released locally.
        for cloid in cloids:
            self._clear_cancel_tracking(cloid)
        self.official_untracked_bot_orders = 0
        return len(cloids)

    @staticmethod
    def _explicit_exchange_error(response: object) -> str | None:
        """Return a definitive top-level exchange rejection, if present."""

        if not isinstance(response, dict):
            return None
        if str(response.get("status") or "").lower() not in {"err", "error"}:
            return None
        detail = response.get("response")
        if isinstance(detail, str) and detail.strip():
            return detail.strip()
        if (
            isinstance(detail, dict)
            and set(detail) == {"error"}
            and isinstance(detail.get("error"), str)
            and detail["error"].strip()
        ):
            return detail["error"].strip()
        return None

    def _adopt_placed_order(
        self,
        intent: OrderIntent,
        cloid: str,
        oid_raw: object,
        action_row: dict,
        *,
        outcome: str,
    ) -> TrackedOrder:
        oid = _positive_order_id(oid_raw)
        tracked = TrackedOrder(
            role=intent.role,
            cloid=cloid,
            is_buy=intent.is_buy,
            price=intent.price,
            qty=intent.qty,
            reduce_only=intent.reduce_only,
            tif=intent.tif,
            created_ts=time.time(),
            oid=oid,
        )
        self.orders_by_role[intent.role] = tracked
        if tracked.oid is not None:
            self._oid_to_role[tracked.oid] = tracked.role
            self._identity_pending_cloids.discard(tracked.cloid)
        else:
            self._identity_pending_cloids.add(tracked.cloid)
        action_row["outcome"] = outcome
        action_row["oid"] = tracked.oid
        return tracked

    def _reserve_uncertain_place(
        self, intent: OrderIntent, cloid: str, action_row: dict
    ) -> None:
        """Reserve a role for a place whose exchange effect is still unknown."""

        self._adopt_placed_order(
            intent,
            cloid,
            None,
            action_row,
            outcome="send_uncertain",
        )
        log.warning(
            "下单 %s 结果仍不确定；已保留本次 cloid/角色且禁止重发",
            intent.role,
        )

    async def _reconcile_uncertain_place(
        self, intent: OrderIntent, cloid: str, action_row: dict
    ) -> str:
        """Resolve one non-idempotent place by cloid without resubmission."""

        for attempt in range(2):
            try:
                status = await self.market.order_status(self.address, cloid)
            except Exception:
                self._reserve_uncertain_place(intent, cloid, action_row)
                return ""
            top_state = str((status or {}).get("status") or "")
            if top_state == "order":
                order_data = (status or {}).get("order") or {}
                inner = order_data.get("order") or {}
                reported_cloid = str(
                    order_data.get("cloid") or inner.get("cloid") or cloid
                ).lower()
                if reported_cloid != cloid.lower():
                    self._reserve_uncertain_place(intent, cloid, action_row)
                    return ""
                state = _order_state(status) or "order"
                self._adopt_placed_order(
                    intent,
                    cloid,
                    order_data.get("oid") or inner.get("oid"),
                    action_row,
                    outcome=f"reconciled_{state}",
                )
                log.info("下单 %s 响应不确定但已在交易所入账；已收编跟踪", intent.role)
                return "adopted"
            if top_state != "unknownOid":
                self._reserve_uncertain_place(intent, cloid, action_row)
                return ""
            if attempt == 0:
                await asyncio.sleep(1.5)
        # unknownOid is a transient lookup result, not a non-submission proof.
        self._reserve_uncertain_place(intent, cloid, action_row)
        return ""

    async def _reconcile_uncertain_places(
        self,
        prepared: list[tuple[OrderIntent, str]],
        action_rows: dict[str, dict],
    ) -> int:
        unresolved = 0
        for intent, cloid in prepared:
            if not await self._reconcile_uncertain_place(
                intent, cloid, action_rows[cloid]
            ):
                unresolved += 1
        return unresolved

    async def place(self, intents: list[OrderIntent], book: OrderBook) -> None:
        if not intents:
            return
        assert self.exchange is not None
        from hyperliquid.utils.types import Cloid

        prepared: list[tuple[OrderIntent, str]] = []
        action_rows: dict[str, dict] = {}
        requests = []
        for intent in intents:
            cloid = self.cloids.next()
            prepared.append((intent, cloid))
            requests.append(
                {
                    "coin": self.meta.coin,
                    "is_buy": intent.is_buy,
                    "sz": intent.qty,
                    "limit_px": intent.price,
                    "order_type": {"limit": {"tif": intent.tif}},
                    "reduce_only": intent.reduce_only,
                    "cloid": Cloid.from_str(cloid),
                }
            )
        for intent, cloid in prepared:
            action_rows[cloid] = self._note_order_event(
                "place",
                durable_intent=True,
                role=intent.role,
                cloid=cloid,
                is_buy=intent.is_buy,
                px=intent.price,
                qty=intent.qty,
                reduce_only=intent.reduce_only,
                tif=intent.tif,
                outcome="submitted",
            )
        # Address-based accounting: a batch of n orders counts as n requests
        # (only the IP weight treats the batch as one).
        self.action_budget.spend(len(requests))
        try:
            response = await self._sdk(self.exchange.bulk_orders, requests)
        except Exception as exc:
            if _http_status_code(exc) == 429:
                # HTTP 429 is an explicit endpoint rejection, not a dropped
                # response after an unknown send.  Do not manufacture oid-less
                # placeholders for orders the server refused.
                self._note_action_rate_limit()
                for row in action_rows.values():
                    row["outcome"] = "rate_limited"
                    row["reason"] = "HTTP 429"
                log.warning("下单命中交易所限流；暂停新增/改单 10 秒")
                return
            # Non-idempotent send: never blind-retry. Query every cloid first.
            await asyncio.sleep(0.5)
            unresolved = await self._reconcile_uncertain_places(
                prepared, action_rows
            )
            if unresolved:
                raise RuntimeError(
                    f"下单结果不确定，已停止且不重试: {unresolved} 笔"
                ) from exc
            return

        explicit_error = self._explicit_exchange_error(response)
        if explicit_error:
            for row in action_rows.values():
                row["outcome"] = "rejected"
                row["reason"] = explicit_error[:160]
            raise RuntimeError(f"批量下单被交易所拒绝: {explicit_error[:240]}")
        if not isinstance(response, dict) or response.get("status") != "ok":
            unresolved = await self._reconcile_uncertain_places(
                prepared, action_rows
            )
            if unresolved:
                raise RuntimeError(
                    f"批量下单顶层响应异常，且有 {unresolved} 笔未决"
                )
            return
        response_body = response.get("response")
        response_data = (
            response_body.get("data") if isinstance(response_body, dict) else None
        )
        statuses = (
            response_data.get("statuses") if isinstance(response_data, dict) else None
        )
        if not isinstance(statuses, list) or len(statuses) != len(prepared):
            unresolved = await self._reconcile_uncertain_places(
                prepared, action_rows
            )
            raise RuntimeError(
                "批量下单响应数量不匹配；"
                f"已逐笔对账，仍有 {unresolved} 笔未决"
            )
        errors = []
        unresolved = 0
        for (intent, cloid), status in zip(prepared, statuses):
            action_row = action_rows[cloid]
            if (
                isinstance(status, dict)
                and set(status) == {"error"}
                and str(status.get("error") or "").strip()
            ):
                errors.append(f"{intent.role}: {status['error']}")
                action_row["outcome"] = "rejected"
                action_row["reason"] = str(status.get("error"))[:120]
                continue
            result_kinds = (
                [
                    kind
                    for kind in ("resting", "filled")
                    if isinstance(status.get(kind), dict)
                ]
                if isinstance(status, dict)
                else []
            )
            if len(result_kinds) == 1 and "error" not in status:
                kind = result_kinds[0]
                result = status[kind]
                self._adopt_placed_order(
                    intent,
                    cloid,
                    result.get("oid"),
                    action_row,
                    outcome=kind,
                )
                continue
            if not await self._reconcile_uncertain_place(
                intent, cloid, action_row
            ):
                unresolved += 1
        if unresolved:
            raise RuntimeError(
                f"批量下单子项响应异常，已对账但仍有 {unresolved} 笔未决"
            )
        if errors:
            log.warning("部分 ALO 未挂上（通常是价格已变化）: %s", " | ".join(errors))

    def _adopt_modified_order(
        self,
        order: TrackedOrder,
        intent: OrderIntent,
        new_cloid: str,
        new_oid: int | None,
    ) -> None:
        """Replace one tracked order's identity after a confirmed modify.

        The old incarnation keeps its OID mapping exactly like a canceled
        order, so a fill that happened just before the modify still lands on
        the correct identity when its userFills row arrives late.
        """
        new_oid = _positive_order_id(new_oid)
        replacement = TrackedOrder(
            role=order.role,
            cloid=new_cloid,
            is_buy=intent.is_buy,
            price=intent.price,
            qty=intent.qty,
            reduce_only=intent.reduce_only,
            tif=intent.tif,
            created_ts=time.time(),
            oid=new_oid,
        )
        self.orders_by_role.pop(order.role, None)
        self._missing_since.pop(order.cloid, None)
        self._recent_fill_fallback_at.pop(order.cloid, None)
        self._deep_fill_fallback_done.discard(order.cloid)
        self._clear_cancel_tracking(order.cloid)
        self._identity_pending_cloids.discard(order.cloid)
        self._uncertain_modify_predecessors.pop(order.cloid, None)
        if order.oid is not None and order.oid != replacement.oid:
            self._retire_order(order)
        self.orders_by_role[order.role] = replacement
        if replacement.oid is not None:
            self._oid_to_role[replacement.oid] = replacement.role
            self._identity_pending_cloids.discard(replacement.cloid)
        else:
            self._identity_pending_cloids.add(replacement.cloid)

    def _reserve_uncertain_modify(
        self,
        order: TrackedOrder,
        intent: OrderIntent,
        new_cloid: str,
        action_row: dict | None,
    ) -> None:
        """Track the replacement identity while a modify effect is unknown."""

        self._adopt_modified_order(order, intent, new_cloid, None)
        self._uncertain_modify_predecessors[new_cloid] = order
        if action_row is not None:
            action_row["outcome"] = "send_uncertain"
            action_row["oid"] = None
        log.warning(
            "改单 %s 结果仍不确定；已保留 replacement cloid/角色且禁止重发",
            order.role,
        )

    async def _resolve_modify_failure(self, order: TrackedOrder) -> str:
        """Resolve one failed/uncertain modify against official state.

        Returns the understood outcome, or "" when the official view is
        inconclusive.  Never places anything here:
        - "open"     -> old order still resting, tracking unchanged
        - "canceled" -> role released (normal replacement follows)
        - "filled"   -> keep mapping; fill reconciliation owns it
        """
        status = await self._order_status_view(order.cloid)
        state = _order_state(status)
        if state in {"", "open"}:
            return "open"
        if state in CANCELED_ORDER_STATES:
            reported_fill = _reported_filled_qty(status)
            if reported_fill > order.filled_qty + self.meta.size_step / 2:
                await self._sync_position_only()
            self._retire_active_order(order)
            return "canceled"
        if state == "filled":
            # Leave the role reserved; _reconcile_missing_tracked applies the
            # bounded fill-reconciliation grace exactly as for any other
            # filled-while-tracked order.
            return "filled"
        return ""

    async def _reconcile_uncertain_modify(
        self,
        order: TrackedOrder,
        intent: OrderIntent,
        new_cloid: str,
        action_row: dict | None = None,
    ) -> str:
        """Resolve a possibly accepted modify without ever resubmitting it.

        A just-created cloid can briefly return ``unknownOid``.  That absence
        is never evidence that a non-idempotent modify was rejected.  Any
        ambiguity replaces the active mapping with an oid-less placeholder for
        the generated cloid, retires the old identity, and fails closed.
        """

        for attempt in range(2):
            try:
                new_status = await self.market.order_status(self.address, new_cloid)
            except Exception:
                self._reserve_uncertain_modify(
                    order, intent, new_cloid, action_row
                )
                return ""
            top_state = str((new_status or {}).get("status") or "")
            if top_state == "order":
                outer = (new_status or {}).get("order") or {}
                inner = outer.get("order") or {}
                reported_cloid = str(
                    outer.get("cloid") or inner.get("cloid") or new_cloid
                ).lower()
                if reported_cloid != new_cloid.lower():
                    self._reserve_uncertain_modify(
                        order, intent, new_cloid, action_row
                    )
                    return ""
                oid_raw = outer.get("oid") or inner.get("oid") or 0
                new_oid = _positive_order_id(oid_raw)
                self._adopt_modified_order(order, intent, new_cloid, new_oid)
                if action_row is not None:
                    action_row["outcome"] = f"reconciled_{_order_state(new_status) or 'order'}"
                    action_row["oid"] = new_oid
                return "adopted"
            if top_state != "unknownOid":
                self._reserve_uncertain_modify(
                    order, intent, new_cloid, action_row
                )
                return ""
            if attempt == 0:
                await asyncio.sleep(1.5)

        self._reserve_uncertain_modify(order, intent, new_cloid, action_row)
        return ""

    async def modify_orders(
        self, pairs: list[tuple[str, OrderIntent]], book: OrderBook
    ) -> None:
        """Atomically reprice tracked roles with batchModify.

        Uses the default (omitted ``always_place``) semantics: the exchange
        only accepts ALO replacements and never places the new order when the
        old one is already gone, so a race with a fill cannot double
        exposure.  A changed price must be treated as a fresh queue position;
        the exchange does not document time-priority preservation.

        A modify the exchange deterministically rejects (while the old order
        keeps resting) is retried at most zero times: the pair falls back to
        cancel+place right here, so a persistent validation failure can never
        loop and drain the address action budget.
        """
        if not pairs:
            return
        assert self.exchange is not None
        from hyperliquid.utils.types import Cloid

        # Defence in depth for callers other than Engine._reconcile: a change
        # of side/TIF/reduce-only is a new order, never a batchModify.  The old
        # implementation deliberately sent the invalid request first and only
        # fell back after an exchange rejection, wasting one address action.
        semantic_replacements: list[tuple[str, OrderIntent]] = []
        compatible_pairs: list[tuple[str, OrderIntent]] = []
        for role, intent in pairs:
            order = self.orders_by_role.get(role)
            if order is not None and (
                order.is_buy != intent.is_buy
                or order.tif != intent.tif
                or order.reduce_only != intent.reduce_only
            ):
                semantic_replacements.append((role, intent))
            else:
                compatible_pairs.append((role, intent))
        if semantic_replacements:
            self.requotes_replaced += len(semantic_replacements)
            before_cancel = allocation_state(self.account)
            await self.cancel_roles([role for role, _ in semantic_replacements])
            if allocation_state(self.account) != before_cancel:
                return  # The engine must allocate again from the updated inventory.
            await self.place(
                [
                    intent
                    for role, intent in semantic_replacements
                    if role not in self.orders_by_role
                ],
                book,
            )
        pairs = compatible_pairs
        if not pairs:
            return

        prepared: list[tuple[TrackedOrder, OrderIntent, str]] = []
        action_rows: dict[str, dict] = {}
        requests = []
        for role, intent in pairs:
            order = self.orders_by_role.get(role)
            if (
                order is None
                or order.oid is None
                or order.cloid in self._cancel_pending_since
            ):
                continue
            if intent.tif != "Alo":
                raise RuntimeError("batchModify 仅用于 ALO 报价单")
            new_cloid = self.cloids.next()
            prepared.append((order, intent, new_cloid))
            requests.append(
                {
                    "oid": order.oid,
                    "order": {
                        "coin": self.meta.coin,
                        "is_buy": intent.is_buy,
                        "sz": intent.qty,
                        "limit_px": intent.price,
                        "order_type": {"limit": {"tif": intent.tif}},
                        "reduce_only": intent.reduce_only,
                        "cloid": Cloid.from_str(new_cloid),
                    },
                }
            )
        if not requests:
            return
        for order, intent, new_cloid in prepared:
            action_rows[new_cloid] = self._note_order_event(
                "modify",
                durable_intent=True,
                role=order.role,
                cloid=new_cloid,
                oid=order.oid,
                previous_cloid=order.cloid,
                is_buy=intent.is_buy,
                px=intent.price,
                qty=intent.qty,
                reduce_only=intent.reduce_only,
                tif=intent.tif,
                prev_px=order.price,
                outcome="submitted",
            )
        self.requotes_modified += len(requests)
        # n modifies in one batch cost n address-based actions.
        self.action_budget.spend(len(requests))
        try:
            response = await self._sdk(
                self.exchange.bulk_modify_orders_new, requests
            )
        except Exception as exc:
            if _http_status_code(exc) == 429:
                # The server explicitly refused the action.  The predecessor
                # remains authoritative, so keep it tracked and pace the next
                # quote mutation instead of creating a false unknown modify.
                self._note_action_rate_limit()
                for row in action_rows.values():
                    row["outcome"] = "rate_limited"
                    row["reason"] = "HTTP 429"
                log.warning("改单命中交易所限流；保留原委托并暂停新增/改单 10 秒")
                return
            # Non-idempotent send with unknown outcome.  Reconcile each pair
            # against official state; never resend blindly.
            await asyncio.sleep(0.5)
            unresolved = 0
            for order, intent, new_cloid in prepared:
                if not await self._reconcile_uncertain_modify(
                    order, intent, new_cloid, action_rows[new_cloid]
                ):
                    unresolved += 1
            if unresolved:
                raise RuntimeError(
                    f"改单结果不确定，已停止且不重试: {unresolved} 笔"
                ) from exc
            return
        explicit_error = self._explicit_exchange_error(response)
        if explicit_error:
            for row in action_rows.values():
                row["outcome"] = "rejected"
                row["reason"] = explicit_error[:160]
            raise RuntimeError(f"批量改单被交易所拒绝: {explicit_error[:240]}")
        if not isinstance(response, dict) or response.get("status") != "ok":
            unresolved = 0
            for order, intent, new_cloid in prepared:
                if not await self._reconcile_uncertain_modify(
                    order, intent, new_cloid, action_rows[new_cloid]
                ):
                    unresolved += 1
            if unresolved:
                raise RuntimeError(
                    f"批量改单顶层响应异常，且有 {unresolved} 笔未决"
                )
            return
        response_body = response.get("response")
        response_data = (
            response_body.get("data") if isinstance(response_body, dict) else None
        )
        statuses = (
            response_data.get("statuses") if isinstance(response_data, dict) else None
        )
        if not isinstance(statuses, list) or len(statuses) != len(prepared):
            # A malformed/truncated response can still follow an accepted
            # non-idempotent batch.  Reconcile every generated cloid before
            # surfacing the protocol error so no role is released blindly.
            unresolved = 0
            for order, intent, new_cloid in prepared:
                if not await self._reconcile_uncertain_modify(
                    order, intent, new_cloid, action_rows[new_cloid]
                ):
                    unresolved += 1
            raise RuntimeError(
                "批量改单响应数量不匹配；"
                f"已逐笔对账，仍有 {unresolved} 笔未决"
        )
        fallback: list[tuple[TrackedOrder, OrderIntent]] = []
        unresolved = 0
        for (order, intent, new_cloid), status in zip(prepared, statuses):
            action_row = action_rows[new_cloid]
            if (
                isinstance(status, dict)
                and set(status) == {"error"}
                and str(status.get("error") or "").strip()
            ):
                # Typical causes: price raced through the ALO band, the old
                # order filled a moment earlier, or the exchange refuses the
                # new order's semantics outright.  Resolve against official
                # state first.
                log.info(
                    "改单被拒 (%s): %s", order.role, str(status.get("error"))[:160]
                )
                action_row["outcome"] = "rejected"
                action_row["reason"] = str(status.get("error"))[:120]
                outcome = await self._resolve_modify_failure(order)
                if not outcome:
                    raise RuntimeError(
                        f"改单被拒且订单 {order.role} 状态无法确认"
                    )
                if outcome == "open":
                    # Deterministic rejection with the old order still
                    # resting: retrying the identical modify next loop would
                    # be rejected forever.  Do one plain cancel+place now.
                    fallback.append((order, intent))
                continue
            result_kinds = (
                [
                    kind
                    for kind in ("resting", "filled")
                    if isinstance(status.get(kind), dict)
                ]
                if isinstance(status, dict)
                else []
            )
            if len(result_kinds) == 1 and "error" not in status:
                kind = result_kinds[0]
                result = status[kind]
                new_oid = _positive_order_id(result.get("oid"))
                self._adopt_modified_order(
                    order, intent, new_cloid, new_oid
                )
                action_row["outcome"] = kind
                action_row["oid"] = new_oid
                continue
            if not await self._reconcile_uncertain_modify(
                order, intent, new_cloid, action_row
            ):
                unresolved += 1
        if unresolved:
            raise RuntimeError(
                f"批量改单子项响应异常，已对账但仍有 {unresolved} 笔未决"
            )
        if fallback:
            self.requotes_replaced += len(fallback)
            before_cancel = allocation_state(self.account)
            await self.cancel_roles([order.role for order, _ in fallback])
            if allocation_state(self.account) != before_cancel:
                return
            replacements = [
                intent
                for order, intent in fallback
                if order.role not in self.orders_by_role
            ]
            if replacements:
                await self.place(replacements, book)

    # ------------------------------------------------------------------
    # Address-based action budget management
    # ------------------------------------------------------------------

    async def refresh_action_budget(self) -> None:
        """Poll userRateLimit on its own clock and top up when configured."""
        now = time.monotonic()
        if now < self._budget_next_poll:
            return
        self._budget_next_poll = now + self.cfg.runtime.budget_poll_sec
        payload = await self.market.user_rate_limit(self._rate_limit_address)
        self.action_budget.apply_official(payload)
        if self.action_budget.state in {"soft", "hard"}:
            log.warning(
                "地址动作额度吃紧: remaining≈%.0f cap=%.0f (cumVlm=%.0f)",
                self.action_budget.remaining,
                self.action_budget.cap,
                self.action_budget.cum_volume,
            )
            await self._maybe_reserve_actions()

    async def _maybe_reserve_actions(self) -> None:
        cfg = self.cfg.runtime
        if not cfg.auto_reserve_actions or self.exchange is None:
            return
        now = time.monotonic()
        if now < self._reserve_cooldown_until:
            return
        day = time.strftime("%Y-%m-%d")
        if day != self._reserved_usd_day:
            self._reserved_usd_day = day
            self._reserved_usd_today = 0.0
        chunk = int(cfg.reserve_actions_chunk)
        cost = chunk * 0.0005
        if self._reserved_usd_today + cost > cfg.reserve_actions_daily_max_usd:
            log.warning(
                "动作额度购买已达当日上限 $%.2f；跳过",
                cfg.reserve_actions_daily_max_usd,
            )
            self._reserve_cooldown_until = now + 3600.0
            return
        self._reserve_cooldown_until = now + 120.0
        try:
            response = await self._sdk(self._reserve_weight_sync, chunk)
        except Exception as exc:
            log.warning("reserveRequestWeight 提交失败: %s", str(exc)[:200])
            return
        if isinstance(response, dict) and response.get("status") == "ok":
            self._reserved_usd_today += cost
            self.action_budget.cap += chunk
            log.info(
                "已购买 %d 个地址动作 (成本 $%.2f，当日累计 $%.2f)",
                chunk,
                cost,
                self._reserved_usd_today,
            )
        else:
            log.warning("reserveRequestWeight 未确认: %s", str(response)[:200])

    def _reserve_weight_sync(self, weight: int):
        """Sign and submit reserveRequestWeight (0.0005 USDC per action)."""
        assert self.exchange is not None
        from hyperliquid.utils.constants import MAINNET_API_URL
        from hyperliquid.utils.signing import get_timestamp_ms, sign_l1_action

        action = {"type": "reserveRequestWeight", "weight": int(weight)}
        timestamp = get_timestamp_ms()
        signature = sign_l1_action(
            self.exchange.wallet,
            action,
            self.exchange.vault_address,
            timestamp,
            self.exchange.expires_after,
            self.exchange.base_url == MAINNET_API_URL,
        )
        return self.exchange._post_action(action, signature, timestamp)

    async def _cancel_cloids(self, cloids: list[str]) -> None:
        if not cloids:
            return
        assert self.exchange is not None
        from hyperliquid.utils.types import Cloid

        cloids = list(dict.fromkeys(cloids))

        async def submit(values: list[str], *, retry: bool) -> None:
            if not values:
                return
            submitted_at = time.monotonic()
            reqs = [
                {"coin": self.meta.coin, "cloid": Cloid.from_str(value)}
                for value in values
            ]
            for value in values:
                self._cancel_pending_since.setdefault(value, submitted_at)
                self._cancel_last_submit_at[value] = submitted_at
                attempt = self._cancel_submit_attempts.get(value, 0) + 1
                self._cancel_submit_attempts[value] = attempt
                if retry:
                    order = next(
                        (
                            item
                            for item in self.orders_by_role.values()
                            if item.cloid == value
                        ),
                        None,
                    )
                    if order is None:
                        order = self._pending_ioc_orders.get(value)
                    fields: dict[str, object] = {
                        "role": order.role if order else "risk:orphan-cancel",
                        "cloid": value,
                        "reason": "official_still_open_retry",
                        "attempt": attempt,
                    }
                    if order is not None:
                        fields.update(
                            oid=order.oid,
                            is_buy=order.is_buy,
                            px=order.price,
                            filled_qty=order.filled_qty,
                        )
                    self._note_order_event("cancel", **fields)
            # Cancels draw from a larger separate allowance, but the official
            # nRequestsUsed counter still includes them; mirror that here.
            self.action_budget.spend(len(values))
            try:
                await self._sdk(self.exchange.bulk_cancel_by_cloid, reqs)
            except Exception:
                # Outcome is unknown. A later retry is allowed only after a
                # fresh official open-orders read still contains this Cloid.
                pass

        async def officially_still_open(values: list[str]) -> set[str]:
            # orderUpdates usually confirms terminal states within one block.
            # Anything unresolved gets one authoritative REST open-orders read.
            unresolved = list(values)
            if self._ws_connection_alive():
                deadline = time.monotonic() + WS_CANCEL_CONFIRM_SEC
                while unresolved:
                    unresolved = [
                        value
                        for value in unresolved
                        if self._ws_terminal_payload(value) is None
                    ]
                    if not unresolved or time.monotonic() >= deadline:
                        break
                    await asyncio.sleep(0.05)
            if not unresolved:
                return set()
            remaining = {
                str(x.get("cloid"))
                for x in await self.market.open_orders(self.address)
                if x.get("coin") == self.meta.coin
            }
            return set(unresolved) & remaining

        # The first submission does not require prior open-order evidence: it
        # is the strategy's requested cancel. Every later submission does.
        new_cloids = [
            value for value in cloids if value not in self._cancel_pending_since
        ]
        await submit(new_cloids, retry=False)

        still_open = await officially_still_open(cloids)
        if not still_open:
            return

        now = time.monotonic()
        oldest = max(
            now - self._cancel_pending_since[value] for value in still_open
        )
        if oldest >= ORDER_RECONCILIATION_GRACE_SEC:
            raise RuntimeError(
                f"有 {len(still_open)} 笔撤单超过 "
                f"{ORDER_RECONCILIATION_GRACE_SEC:.0f} 秒仍未确认；"
                "为避免重复挂单已停机"
            )

        retry_candidates = [
            value
            for value in still_open
            if self._cancel_submit_attempts.get(value, 1)
            < CANCEL_MAX_SUBMIT_ATTEMPTS
            and now
            - self._cancel_last_submit_at.get(
                value, self._cancel_pending_since[value]
            )
            >= CANCEL_RETRY_INTERVAL_SEC
        ]
        retry_cloids: list[str] = []
        terminal_cloids: set[str] = set()
        for value in retry_candidates:
            status = await self._order_status_view(value)
            state = _order_state(status)
            if state == "open":
                retry_cloids.append(value)
            elif state == "filled" or state in CANCELED_ORDER_STATES:
                terminal_cloids.add(value)
            # Unknown or contradictory status remains pending, but is never a
            # sufficient second proof for another signed cancel.
        still_open -= terminal_cloids
        if not still_open:
            return
        # Status probes above await network I/O. Recheck every mutable gate
        # without another await before submit() claims the attempt, preventing
        # concurrent reconciliation loops from double-sending a retry.
        claim_now = time.monotonic()
        retry_cloids = [
            value
            for value in retry_cloids
            if self._ws_terminal_payload(value) is None
            and self._cancel_submit_attempts.get(value, 1)
            < CANCEL_MAX_SUBMIT_ATTEMPTS
            and claim_now
            - self._cancel_last_submit_at.get(
                value, self._cancel_pending_since[value]
            )
            >= CANCEL_RETRY_INTERVAL_SEC
            and claim_now - self._cancel_pending_since[value]
            < ORDER_RECONCILIATION_GRACE_SEC
        ]
        if retry_cloids:
            # This is the only retry gate: frontendOpenOrders and orderStatus
            # both proved each exact Cloid open, the cadence has elapsed, and
            # the per-order attempt/time caps have not been reached.
            await submit(retry_cloids, retry=True)
            still_open = await officially_still_open(list(still_open))
            if not still_open:
                return
            oldest = max(
                time.monotonic() - self._cancel_pending_since[value]
                for value in still_open
            )
            if oldest >= ORDER_RECONCILIATION_GRACE_SEC:
                raise RuntimeError(
                    f"有 {len(still_open)} 笔撤单超过 "
                    f"{ORDER_RECONCILIATION_GRACE_SEC:.0f} 秒仍未确认；"
                    "为避免重复挂单已停机"
                )

        max_attempts = max(
            self._cancel_submit_attempts.get(value, 1) for value in still_open
        )
        # orderUpdates usually confirms terminal states within one block.
        # Absence from frontendOpenOrders is not yet a terminal result:
        # cancel_roles retains the mapping until orderStatus also reconciles.
        raise PendingCancelReconciliation(
            f"有 {len(still_open)} 笔撤单等待官方确认；"
            f"已保留映射并暂停替换（{oldest:.0f}/"
            f"{ORDER_RECONCILIATION_GRACE_SEC:.0f} 秒，"
            f"已提交最多 {max_attempts}/{CANCEL_MAX_SUBMIT_ATTEMPTS} 次）"
        )

    async def cancel_roles(self, roles: Iterable[str]) -> None:
        selected = [self.orders_by_role[r] for r in roles if r in self.orders_by_role]
        # Record the first address action, not reconciliation polls. Bounded
        # retries are recorded inside _cancel_cloids only when actually sent.
        new_cloids = {
            order.cloid
            for order in selected
            if order.cloid not in self._cancel_pending_since
        }
        for order in selected:
            if order.cloid not in new_cloids:
                continue
            self._note_order_event(
                "cancel",
                role=order.role,
                cloid=order.cloid,
                oid=order.oid,
                is_buy=order.is_buy,
                px=order.price,
                filled_qty=order.filled_qty,
            )
        await self._cancel_cloids([x.cloid for x in selected])
        position_refresh_required = False
        for order in selected:
            # orderStatus is low weight. Avoid a high-weight userFills request
            # for a clean zero-fill cancellation. If the terminal status
            # reports a fill, retain the mapping until userFills catches up so
            # volume and wear evidence cannot be silently discarded.
            # Poll a new disagreement inline for up to 15 seconds.  Later
            # loops return a PendingOrderReconciliation immediately instead
            # of blocking and, crucially, do not count the same propagation
            # delay as consecutive strategy errors.
            deadline = time.monotonic() + (
                15.0 if order.cloid not in self._missing_since else 0.0
            )
            while order.role in self.orders_by_role:
                status = await self._order_status_view(order.cloid)
                terminal = _order_state(status)
                reported_fill = _reported_filled_qty(status)
                if terminal in CANCELED_ORDER_STATES:
                    if reported_fill > order.filled_qty + self.meta.size_step / 2:
                        # A canceled order's inner `sz` is not documented as
                        # cumulative fill. Do not manufacture a fill or stall
                        # for minutes based on origSz-sz. Refresh authoritative
                        # exposure once and keep the retired OID so an actual
                        # late userFills row is still recorded exactly.
                        position_refresh_required = True
                    break
                if terminal == "filled":
                    if self._reduce_only_fill_is_fully_reconciled(
                        order, reported_fill
                    ):
                        log.info(
                            "减仓订单 %s 已按剩余仓位成交完毕；官方累计成交量与本地明细一致",
                            order.role,
                        )
                        break
                    first_missing = self._missing_since.setdefault(
                        order.cloid, time.monotonic()
                    )
                    elapsed = time.monotonic() - first_missing
                    if elapsed >= FILL_RECONCILIATION_GRACE_SEC:
                        # Last chance before failing closed: a clipped
                        # reduce-only remainder has no fills to wait for.
                        if await self._reduce_only_clip_released(
                            order, elapsed, force=True
                        ):
                            break
                        raise RuntimeError(
                            f"订单 {order.role} 已显示 filled，但超过 "
                            f"{FILL_RECONCILIATION_GRACE_SEC:.0f} 秒仍未收到成交明细；"
                            "为避免重复下单已停机"
                        )
                    await self._maybe_sync_recent_fills(order, elapsed)
                    if order.role not in self.orders_by_role:
                        continue
                    if self._reduce_only_fill_is_fully_reconciled(
                        order, reported_fill
                    ):
                        log.info(
                            "减仓订单 %s 已按剩余仓位成交完毕；官方累计成交量与本地明细一致",
                            order.role,
                        )
                        break
                    if await self._reduce_only_clip_released(order, elapsed):
                        break
                    if time.monotonic() < deadline:
                        await asyncio.sleep(0.5)
                        await self._sync_fills()
                        continue
                    raise PendingFillReconciliation(
                        f"订单 {order.role} 已显示 filled，本地明细暂未到达；"
                        f"已保留映射并暂停替换（{elapsed:.0f}/"
                        f"{FILL_RECONCILIATION_GRACE_SEC:.0f} 秒）"
                    )
                if terminal in {"", "open"} and time.monotonic() < deadline:
                    # frontendOpenOrders has already confirmed the cloid is no
                    # longer open, but orderStatus may lag briefly and still
                    # return its previous `open` snapshot. Keep the role
                    # reserved and wait for a terminal state; never replace it
                    # while the two official views disagree.
                    await asyncio.sleep(0.5)
                    continue
                if terminal in {"", "open"}:
                    first_missing = self._missing_since.setdefault(
                        order.cloid,
                        self._cancel_pending_since.get(order.cloid, time.monotonic()),
                    )
                    elapsed = time.monotonic() - first_missing
                    if elapsed >= ORDER_RECONCILIATION_GRACE_SEC:
                        raise RuntimeError(
                            f"订单 {order.role} 撤销后官方视图持续矛盾超过 "
                            f"{ORDER_RECONCILIATION_GRACE_SEC:.0f} 秒；"
                            "为避免重复下单已停机"
                        )
                    raise PendingCancelReconciliation(
                        f"订单 {order.role} 已从开放委托消失，但 orderStatus 仍为 "
                        f"{terminal or 'unknown'}；已保留映射且不会重发撤单"
                        f"（{elapsed:.0f}/{ORDER_RECONCILIATION_GRACE_SEC:.0f} 秒）"
                    )
                raise RuntimeError(
                    f"订单 {order.role} 撤销后终态无法安全对账: "
                    f"{terminal or 'unknown'}"
                )
            self._retire_active_order(order)
        if position_refresh_required:
            await self._sync_position_only()

    def _ingest_fill_row(self, raw: dict) -> int:
        """Apply one official fill row (REST page or WS push).

        Returns the row's timestamp when it belongs to this market (so REST
        callers can advance their cursor), else 0.  Deduplication by trade id
        makes this safe to call from overlapping sources.
        """
        if raw.get("coin") != self.meta.coin:
            return 0
        row_time = int(raw.get("time") or 0)
        oid = int(raw.get("oid") or 0)
        pending_ioc = self._pending_ioc_by_identity(
            cloid=str(raw.get("cloid") or ""), oid=oid
        )
        if pending_ioc is not None:
            self._mark_ioc_proven(pending_ioc)
            if oid:
                self._register_ioc_identity(pending_ioc, oid)
        active_order = self._bind_active_order_identity(
            cloid=str(raw.get("cloid") or ""), oid=oid
        )
        if active_order is not None:
            role = active_order.role
        else:
            role = self._oid_to_role.get(oid)
        if role is None:
            return row_time
        raw_tid = raw.get("tid")
        if raw_tid is None or isinstance(raw_tid, bool) or str(raw_tid) == "":
            raise RuntimeError("官方成交缺少 tid；不能猜测唯一标识或合并部分成交")
        raw_id = str(raw_tid)
        if raw_id in self._seen_fills:
            return row_time
        order = self.orders_by_role.get(role)
        is_active_order = order is not None and order.oid == oid
        if not is_active_order:
            retired = self._retired_orders_by_oid.get(oid)
            order = retired[0] if retired is not None else None
        if order is None:
            return row_time

        def known_number(name: str) -> float | None:
            value = raw.get(name)
            if value is None or isinstance(value, bool) or value == "":
                return None
            try:
                result = float(value)
            except (TypeError, ValueError):
                return None
            return result if math.isfinite(result) else None

        price, quantity = known_number("px"), known_number("sz")
        side = str(raw.get("side") or "").upper()
        if price is None or quantity is None or price <= 0 or quantity <= 0 or side not in {"B", "A"} or row_time <= 0:
            raise RuntimeError("官方成交方向、价格、数量或时间无效；保留映射等待完整明细")
        # Commit the dedup ID only after parsing succeeds. A corrected row for
        # the same trade must remain ingestible after a malformed WS message.
        self._seen_fills.add(raw_id)
        self._seen_fill_order.append(raw_id)
        fee_value = known_number("fee")
        fee_token = str(raw.get("feeToken") or "").strip() or None
        # fee already INCLUDES builderFee. Never add it a second time, and
        # never label an amount charged in an unknown token as USD.
        fee_known = fee_value is not None and fee_token == "USDC"
        fee = fee_value if fee_known else 0.0
        self.account.fee_accounting_complete &= fee_known
        own, referred = self.fees.projected_entropy_rebate(fee)
        pnl_value = known_number("closedPnl")
        pnl = pnl_value if pnl_value is not None else 0.0
        self.account.realized_accounting_complete &= pnl_value is not None
        fill = Fill(
            fill_id=raw_id,
            order_role=role,
            cloid=order.cloid,
            oid=oid,
            is_buy=side == "B",
            price=price,
            qty=quantity,
            fee_usd=fee,
            liquidity="taker" if bool(raw.get("crossed")) else "maker",
            time_ms=row_time,
            realized_pnl_delta=pnl,
            projected_self_rebate=own,
            projected_referred_benefit=referred,
            start_position_qty=known_number("startPosition"),
            fee_token=fee_token,
            fee_known=fee_known,
            realized_pnl_known=pnl_value is not None,
        )
        self._fills.append(fill)
        order.filled_qty += fill.qty
        # The fill feed can lead clearinghouseState by several seconds.
        # Apply the signed fill immediately so strategy allocation cannot
        # repost another same-side order against a stale position and
        # overshoot the configured cap.  The next authoritative account
        # sync will replace this estimate with exchange state.
        # A clearinghouse push can arrive BEFORE its userFills event. Its
        # official timestamp already includes that fill, so incrementing again
        # would double the position until another account snapshot arrived.
        if row_time > self._position_snapshot_ms:
            self.account.position_qty += fill.qty if fill.is_buy else -fill.qty
        self._last_applied_fill_ms = max(self._last_applied_fill_ms, row_time)
        self._session_realized += pnl
        self.account.fees_paid += fee
        self.account.projected_self_rebate += own
        self.account.projected_referred_benefit += referred
        self.account.volume_usd += fill.notional
        if is_active_order and order.filled_qty + self.meta.size_step / 2 >= order.qty:
            self._retire_active_order(order)
        elif not is_active_order:
            active = self.orders_by_role.get(role)
            predecessor = (
                self._uncertain_modify_predecessors.get(active.cloid)
                if active is not None
                else None
            )
            if (
                active is not None
                and active.oid is None
                and predecessor is order
                and order.filled_qty + self.meta.size_step / 2 >= order.qty
            ):
                # batchModify is conditional on the predecessor still being
                # modifiable.  Its complete fill is positive evidence that an
                # ambiguous replacement could not have been installed.
                log.info("改单未决已由原委托完整成交自动解除: %s", role)
                self._retire_active_order(active)
        if len(self._seen_fills) > 20_000:
            while len(self._seen_fill_order) > 10_000:
                self._seen_fills.discard(self._seen_fill_order.popleft())
        return row_time

    async def _sync_fills(
        self,
        *,
        use_recent_view: bool = False,
        start_ms: int | None = None,
    ) -> None:
        # Keep a generous overlap because userFills and open-order state are not
        # guaranteed to become visible in the same request cycle.
        self._prune_retired_orders()
        if use_recent_view:
            fills = await self.market.recent_fills(self.address)
        else:
            fill_start_ms = (
                int(start_ms)
                if start_ms is not None
                else self._last_fill_ms - 10_000
            )
            fills = await self.market.fills_since(self.address, fill_start_ms)
        max_time = self._last_fill_ms
        for raw in sorted(fills, key=lambda x: int(x.get("time") or 0)):
            # Separate brokers for multiple markets can share one account.
            # userFillsByTime returns all markets, so a newer fill from ANTH
            # must not advance SNDK's cursor past a delayed SNDK row (or vice
            # versa).  Only the broker's own market is allowed to move its
            # incremental cursor.
            max_time = max(max_time, self._ingest_fill_row(raw))
        self._last_fill_ms = max_time + 1

    async def _maybe_sync_recent_fills(
        self, order: TrackedOrder, elapsed: float
    ) -> None:
        """Recover a delayed fill before the bounded fail-closed deadline.

        ``userFills`` is a useful alternate view, but it is a bounded recent
        page and can still omit a row that arrived late.  Near the end of the
        reconciliation grace period, query ``userFillsByTime`` once from the
        tracked order's creation time.  This lookup is read-only, deduplicated
        by fill id, and cannot create a replacement order; failure still falls
        through to the existing 300-second hard stop.
        """
        if elapsed < 10.0:
            return
        if not callable(getattr(self.market, "recent_fills", None)):
            return
        now = time.monotonic()
        last = self._recent_fill_fallback_at.get(order.cloid, 0.0)
        if now - last >= 15.0:
            self._recent_fill_fallback_at[order.cloid] = now
            await self._sync_fills(use_recent_view=True)
        if (
            elapsed >= 45.0
            and order.cloid not in self._deep_fill_fallback_done
            and order.role in self.orders_by_role
            and callable(getattr(self.market, "fills_since", None))
        ):
            self._deep_fill_fallback_done.add(order.cloid)
            start_ms = max(int(order.created_ts * 1000) - 60_000, 0)
            log.warning(
                "订单 %s 成交明细延迟 %.0f 秒；执行一次按订单时间回溯的官方补录",
                order.role,
                elapsed,
            )
            await self._sync_fills(start_ms=start_ms)

    async def _sync_open_orders(self, open_raw: list | None = None) -> None:
        if open_raw is None:
            open_raw = await self.market.open_orders(self.address)
        self._classify_official_orders(open_raw)
        visible_retired = set(self._visible_retired_orders)
        recovered = (
            set(self._retired_open_since)
            - visible_retired
            - self._expired_retired_order_cloids
        )
        for cloid in recovered:
            self._retired_open_since.pop(cloid, None)
        if recovered:
            log.info("已完成订单与开放委托快照的短暂矛盾已恢复")
        for cloid in visible_retired:
            self._retired_open_since.setdefault(cloid, time.monotonic())
        if visible_retired:
            # Do not expose identifiers and do not release the strategy loop to
            # place replacements. If this persists past the bounded grace, the
            # next classification promotes it to a genuine bot orphan so the
            # existing fail-closed cancellation path handles it.
            raise PendingOrderReconciliation(self._retired_open_pending_message())
        open_cloids = {
            str(x.get("cloid"))
            for x in open_raw
            if x.get("coin") == self.meta.coin and x.get("cloid")
        }
        await self._reconcile_missing_tracked(open_cloids, set())

    async def _reconcile_missing_tracked(
        self, open_cloids: set[str], open_oids: set[int]
    ) -> None:
        # Fill sync runs first, but the fill feed can still lag the open-order
        # endpoint. Never interpret "not open" as "safe to replace": doing so
        # could duplicate a filled order. Confirm terminal state first and halt
        # if a reported fill does not arrive within a bounded grace period.
        position_refresh_required = False
        for role, order in list(self.orders_by_role.items()):
            is_open = order.cloid in open_cloids or (
                order.oid is not None and order.oid in open_oids
            )
            if order.tif == "Alo" and not is_open:
                status = await self._order_status_view(order.cloid)
                if str((status or {}).get("status") or "") == "order":
                    outer = (status or {}).get("order") or {}
                    inner = outer.get("order") or {}
                    self._bind_active_order_identity(
                        cloid=str(
                            outer.get("cloid")
                            or inner.get("cloid")
                            or order.cloid
                        ),
                        oid=outer.get("oid") or inner.get("oid"),
                    )
                terminal_key = _order_state(status)
                reported_fill = _reported_filled_qty(status)
                if terminal_key == "filled":
                    if self._reduce_only_fill_is_fully_reconciled(
                        order, reported_fill
                    ):
                        log.info(
                            "减仓订单 %s 已按剩余仓位成交完毕；官方累计成交量与本地明细一致",
                            role,
                        )
                        self._retire_active_order(order)
                        continue
                    first_missing = self._missing_since.setdefault(
                        order.cloid, time.monotonic()
                    )
                    elapsed = time.monotonic() - first_missing
                    if elapsed >= FILL_RECONCILIATION_GRACE_SEC:
                        # Last chance before failing closed: a clipped
                        # reduce-only remainder has no fills to wait for.
                        if await self._reduce_only_clip_released(
                            order, elapsed, force=True
                        ):
                            continue
                        raise RuntimeError(
                            f"订单 {role} 已显示 filled，但超过 "
                            f"{FILL_RECONCILIATION_GRACE_SEC:.0f} 秒仍未收到成交明细；"
                            "为避免重复下单已停机"
                        )
                    await self._maybe_sync_recent_fills(order, elapsed)
                    if role not in self.orders_by_role:
                        continue
                    if self._reduce_only_fill_is_fully_reconciled(
                        order, reported_fill
                    ):
                        log.info(
                            "减仓订单 %s 已按剩余仓位成交完毕；官方累计成交量与本地明细一致",
                            role,
                        )
                        self._retire_active_order(order)
                        continue
                    if await self._reduce_only_clip_released(order, elapsed):
                        continue
                    raise PendingFillReconciliation(
                        f"订单 {role} 已显示 filled，本地明细暂未到达；"
                        f"已保留映射并暂停替换（{elapsed:.0f}/"
                        f"{FILL_RECONCILIATION_GRACE_SEC:.0f} 秒）"
                    )
                if terminal_key in CANCELED_ORDER_STATES:
                    if reported_fill > order.filled_qty + self.meta.size_step / 2:
                        position_refresh_required = True
                    self.orders_by_role.pop(role, None)
                    self._missing_since.pop(order.cloid, None)
                    self._clear_cancel_tracking(order.cloid)
                    self._identity_pending_cloids.discard(order.cloid)
                    self._retire_order(order)
                    continue
                if terminal_key in {"", "open"}:
                    first_missing = self._missing_since.setdefault(
                        order.cloid, time.monotonic()
                    )
                    elapsed = time.monotonic() - first_missing
                    # A rate-limited/ambiguous modify can leave an oid-less
                    # replacement while the predecessor's final fill arrives
                    # on the separate userFills feed.  Fetch that evidence
                    # before escalating the harmless status eviction.
                    await self._maybe_sync_recent_fills(order, elapsed)
                    if role not in self.orders_by_role:
                        continue
                    if elapsed <= 15.0:
                        # Do not replace the order while frontendOpenOrders and
                        # orderStatus disagree. A later sync must observe a
                        # terminal state or fail closed after the grace period.
                        continue
                    if elapsed < ORDER_RECONCILIATION_GRACE_SEC:
                        raise PendingOrderReconciliation(
                            f"订单 {role} 从开放委托消失，但 orderStatus 仍为 "
                            f"{terminal_key or 'unknown'}；已保留映射并暂停替换"
                            f"（{elapsed:.0f}/{ORDER_RECONCILIATION_GRACE_SEC:.0f} 秒）"
                        )
                    raise RuntimeError(
                        f"订单 {role} 从开放委托消失且官方视图持续矛盾超过 "
                        f"{ORDER_RECONCILIATION_GRACE_SEC:.0f} 秒；"
                        "为避免重复下单已停机"
                    )
                raise RuntimeError(
                    f"订单 {role} 从开放委托消失且终态无法确认: "
                    f"{terminal_key or 'unknown'}"
                )
            if self._missing_since.pop(order.cloid, None) is not None:
                log.info(
                    "订单状态对账恢复: 订单 %s 已重新出现在官方开放委托",
                    role,
                )
        if position_refresh_required:
            await self._sync_position_only()

    async def _sync_account(self, book: OrderBook) -> None:
        del book
        if self.account_abstraction == "unifiedAccount":
            state, spot_state, active = await asyncio.gather(
                self.market.account_state(self.address),
                self.market.spot_state(self.address),
                self.market.active_asset_data(self.address),
            )
        else:
            state = await self.market.account_state(self.address)
            spot_state = {}
            active = {}
        self._apply_account_payload(state, spot_state, active)

    def _apply_account_payload(
        self, state: dict, spot_state: dict, active: dict
    ) -> None:
        positions = _validated_positions(state)
        snapshot_ms = self._check_position_snapshot_time(state)
        next_account = replace(self.account)
        position = None
        for item in positions:
            p = item.get("position") or {}
            if p.get("coin") == self.meta.coin:
                position = p
                break
        if position:
            leverage_source = (
                active
                if (active.get("leverage") or {})
                else position
            )
            self._update_official_leverage(leverage_source)
            next_account.position_qty = float(position.get("szi") or 0.0)
            next_account.entry_px = float(position.get("entryPx") or 0.0)
            next_account.unrealized_pnl = float(position.get("unrealizedPnl") or 0.0)
            liq = position.get("liquidationPx")
            next_account.liquidation_px = float(liq) if liq not in (None, "") else None
        else:
            if active:
                self._update_official_leverage(active)
            next_account.position_qty = 0.0
            next_account.entry_px = 0.0
            next_account.unrealized_pnl = 0.0
            next_account.liquidation_px = None
        next_account.account_abstraction = self.account_abstraction
        if self.account_abstraction == "unifiedAccount":
            usdc = next(
                (
                    item
                    for item in spot_state.get("balances") or []
                    if item.get("coin") == "USDC"
                ),
                None,
            )
            if not usdc or usdc.get("total") in (None, ""):
                raise RuntimeError("统一账户 spotClearinghouseState 缺少 USDC 余额")
            spot_total = float(usdc["total"])
            total_unrealized = sum(
                float((item.get("position") or {}).get("unrealizedPnl") or 0.0)
                for item in positions
            )
            available = active.get("availableToTrade") or []
            if len(available) != 2:
                raise RuntimeError("统一账户 activeAssetData 缺少双边可交易额")
            buy_available = float(available[0])
            sell_available = float(available[1])
            next_account.equity_actual = spot_total + total_unrealized
            next_account.available_to_buy_usd = buy_available
            next_account.available_to_sell_usd = sell_available
            next_account.withdrawable = min(buy_available, sell_available)
            next_account.collateral_source = "unified_spot_usdc"
        else:
            summary = state.get("marginSummary") or {}
            equity = summary.get("accountValue")
            if equity in (None, ""):
                raise RuntimeError("Entropy DEX clearinghouseState 缺少 accountValue")
            next_account.equity_actual = float(equity)
            withdrawable = state.get("withdrawable")
            next_account.withdrawable = (
                float(withdrawable) if withdrawable not in (None, "") else None
            )
            next_account.available_to_buy_usd = next_account.withdrawable
            next_account.available_to_sell_usd = next_account.withdrawable
            next_account.collateral_source = "io_dex"
        for field in ("equity_actual", "withdrawable", "available_to_buy_usd", "available_to_sell_usd"):
            value = getattr(next_account, field)
            if value is not None and not math.isfinite(value):
                raise RuntimeError(f"官方账户 {field} 无效；保留上次完整账户快照")
        next_account.realized_pnl = self._session_realized
        self.account.__dict__.update(vars(next_account))
        if snapshot_ms:
            self._position_snapshot_ms = snapshot_ms

    async def sync(self, book: OrderBook, *, force: bool = False) -> None:
        now = time.monotonic()
        if self._retired_open_since and not force:
            oldest = min(self._retired_open_since.values())
            pending_expired = now - oldest >= ORDER_RECONCILIATION_GRACE_SEC
            refresh_due = now - self._last_fill_sync >= self.cfg.runtime.fill_refresh_sec
            if not pending_expired and not refresh_due:
                raise PendingOrderReconciliation(
                    self._retired_open_pending_message()
                )
        ws_view = self._ws_open_view()
        poll_due = now - self._last_fill_sync >= self.cfg.runtime.fill_refresh_sec
        sweep_due = now - self._last_rest_sweep >= self.cfg.runtime.rest_sweep_sec
        # REST is required for: explicit force, the periodic authoritative
        # sweep (classification, foreign-order detection, fill backfill),
        # rechecking a pending retired-open contradiction, and any moment the
        # push snapshot is stale/absent.
        rest_needed = (
            force
            or sweep_due
            or (bool(self._retired_open_since) and poll_due)
            or (ws_view is None and poll_due)
        )
        if rest_needed:
            # frontendOpenOrders is half the weight of userFillsByTime. Poll
            # the cheap view first and fetch the high-weight fill page only
            # when a tracked order disappeared, on an explicit force, or as a
            # periodic safety sweep. A missing order is still reconciled in
            # this same cycle, so turnover safety is not weakened.
            open_raw = await self.market.open_orders(self.address)
            open_cloids = {
                str(item.get("cloid"))
                for item in open_raw
                if item.get("coin") == self.meta.coin and item.get("cloid")
            }
            has_missing_order = any(
                order.tif == "Alo" and order.cloid not in open_cloids
                for order in self.orders_by_role.values()
            )
            full_fill_due = now - self._last_full_fill_sync >= 60.0
            if force or has_missing_order or full_fill_due:
                await self._sync_fills()
                self._last_full_fill_sync = time.monotonic()
            # Advance the poll clock even when the bounded reconciliation guard
            # below raises PendingOrderReconciliation; otherwise the main loop
            # would hammer the official endpoints on every book event.
            self._last_fill_sync = time.monotonic()
            self._last_rest_sweep = time.monotonic()
            await self._sync_open_orders(open_raw)
        elif ws_view is not None:
            # Push-based steady state: zero REST weight.  Fills and order
            # events arrived over the websocket already; this pass only has
            # to reconcile any tracked order that vanished from the push
            # snapshot (usually because it just filled).
            open_cloids, open_oids = ws_view
            self._prune_retired_orders()
            self._last_fill_sync = time.monotonic()
            await self._reconcile_missing_tracked(open_cloids, open_oids)
        if force or now - self._last_account_sync >= self.cfg.runtime.account_refresh_sec:
            await self._sync_account(book)
            self._last_account_sync = time.monotonic()

    @staticmethod
    def _ioc_status_oid(status: dict | None) -> int | None:
        outer = (status or {}).get("order") or {}
        inner = outer.get("order") or {}
        raw = outer.get("oid") or inner.get("oid")
        return _positive_order_id(raw)

    @staticmethod
    def _ioc_status_cloid(status: dict | None) -> str:
        outer = (status or {}).get("order") or {}
        inner = outer.get("order") or {}
        return str(outer.get("cloid") or inner.get("cloid") or "").lower()

    def _mark_ioc_proven(self, order: TrackedOrder) -> None:
        """Persist the monotonic fact that this IOC reached official state."""

        self._proven_ioc_cloids.add(order.cloid)

    def _pending_ioc_by_identity(
        self, *, cloid: str = "", oid: object = None
    ) -> TrackedOrder | None:
        """Find an unresolved IOC from either official identity.

        Push and fill payloads can arrive before the synchronous request
        returns.  Joining them here makes official existence monotonic and
        prevents a later ``unknownOid`` lookup from opening a retry window.
        """

        normalized_cloid = str(cloid or "").lower()
        if normalized_cloid:
            direct = self._pending_ioc_orders.get(normalized_cloid)
            if direct is not None:
                return direct
            for pending in self._pending_ioc_orders.values():
                if pending.cloid.lower() == normalized_cloid:
                    return pending
        try:
            normalized_oid = int(oid or 0)
        except (TypeError, ValueError):
            normalized_oid = 0
        if normalized_oid:
            for pending in self._pending_ioc_orders.values():
                if pending.oid == normalized_oid:
                    return pending
        return None

    def _register_ioc_identity(self, order: TrackedOrder, oid: int) -> None:
        """Retain a terminal IOC identity until delayed userFills arrives."""

        self._mark_ioc_proven(order)
        existing = self._retired_orders_by_oid.get(oid)
        first_registration = existing is None or existing[0].cloid != order.cloid
        order.oid = oid
        self._retire_order(order)
        if first_registration:
            # This is identity metadata, not another exchange action.  It is
            # persisted so a stopped-process recovery can preserve the IOC's
            # real role instead of misclassifying it as a passive quote.
            self._note_order_event(
                "ioc_identity",
                role=order.role,
                cloid=order.cloid,
                oid=oid,
                is_buy=order.is_buy,
                px=order.price,
                qty=order.qty,
                reduce_only=True,
                tif="Ioc",
            )

    async def _query_ioc_status(self, order: TrackedOrder) -> str:
        """Resolve an IOC from every available identity without regression.

        ``unknownOid`` is only a point-in-time indexing result; the exchange
        does not document it as proof that a non-idempotent send never landed.
        It therefore never releases a pending IOC.  When an oid is known, both
        numeric-oid and cloid views are collected (oid first), and conflicting
        identities/states remain unresolved instead of picking the first row.
        """

        expected_cloid = order.cloid.lower()
        for attempt in range(2):
            payloads: list[dict] = []
            ws_payload = self._ws_terminal_payload(order.cloid)
            if isinstance(ws_payload, dict):
                payloads.append(ws_payload)

            lookups: list[str | int] = (
                [order.oid, order.cloid]
                if order.oid is not None
                else [order.cloid]
            )
            index = 0
            while index < len(lookups):
                lookup = lookups[index]
                index += 1
                try:
                    payload = await self.market.order_status(self.address, lookup)
                except Exception:
                    continue
                if str((payload or {}).get("status") or "") != "order":
                    continue
                payloads.append(payload)
                discovered_oid = self._ioc_status_oid(payload)
                if discovered_oid is not None and discovered_oid not in lookups:
                    # A cloid view can reveal the numeric identity. Verify it
                    # in the same reconciliation round before releasing state.
                    lookups.append(discovered_oid)

            if payloads:
                self._mark_ioc_proven(order)
                identity_conflict = False
                candidate_oids: set[int] = set()
                states: set[str] = set()
                for payload in payloads:
                    reported_cloid = self._ioc_status_cloid(payload)
                    reported_oid = self._ioc_status_oid(payload)
                    if reported_cloid and reported_cloid != expected_cloid:
                        identity_conflict = True
                    if reported_oid is not None:
                        candidate_oids.add(reported_oid)
                        if order.oid is not None and reported_oid != order.oid:
                            identity_conflict = True
                    state = _order_state(payload)
                    if state:
                        states.add(state)

                if identity_conflict or len(candidate_oids) > 1:
                    return "unknown"
                if candidate_oids:
                    self._register_ioc_identity(order, next(iter(candidate_oids)))
                if len(states) != 1:
                    # open-vs-terminal or filled-vs-canceled contradictions
                    # retain the pending guard; never choose a convenient view.
                    return "unknown"
                state = next(iter(states))
                if state == "open":
                    return state
                if state == "filled" or state in CANCELED_ORDER_STATES:
                    # Terminal rows need a numeric identity so delayed fills
                    # can be joined to their true taker role.
                    return state if order.oid is not None else "unknown"
                return "unknown"
            if attempt == 0:
                await asyncio.sleep(1.5)
        return "unknown"

    async def _cancel_abnormal_resting_ioc(self, order: TrackedOrder) -> str:
        """Cancel an IOC that the exchange unexpectedly reports as resting."""

        self._mark_ioc_proven(order)
        if order.cloid not in self._cancel_pending_since:
            self._note_order_event(
                "cancel",
                role=order.role,
                cloid=order.cloid,
                oid=order.oid,
                is_buy=order.is_buy,
                px=order.price,
                qty=order.qty,
                reduce_only=True,
                reason="abnormal_ioc_resting",
            )
        try:
            await self._cancel_cloids([order.cloid])
        except Exception as exc:
            self._pending_ioc_orders[order.cloid] = order
            raise UncertainIocOutcome(
                f"IOC {order.role} 异常挂单后撤销结果不确定；"
                "已保留 cloid 且禁止重发"
            ) from exc
        state = await self._query_ioc_status(order)
        if state == "filled" or state in CANCELED_ORDER_STATES:
            self._clear_cancel_tracking(order.cloid)
            self._pending_ioc_orders.pop(order.cloid, None)
            return state
        self._pending_ioc_orders[order.cloid] = order
        raise UncertainIocOutcome(
            f"IOC {order.role} 异常挂单尚未确认终态；"
            "已保留 cloid 且禁止重发"
        )

    async def _resolve_ioc_after_uncertain_send(self, order: TrackedOrder) -> str:
        state = await self._query_ioc_status(order)
        if state == "open":
            return await self._cancel_abnormal_resting_ioc(order)
        if state == "filled" or state in CANCELED_ORDER_STATES:
            self._pending_ioc_orders.pop(order.cloid, None)
            return state
        self._pending_ioc_orders[order.cloid] = order
        raise UncertainIocOutcome(
            f"IOC {order.role} 发送结果不确定；"
            "已按 cloid 查询但官方状态仍无法确认，禁止重发"
        )

    async def _reconcile_pending_iocs(self) -> None:
        """Block every new IOC until older uncertain cloIds are resolved."""

        resolved_effect = False
        for order in list(self._pending_ioc_orders.values()):
            state = await self._resolve_ioc_after_uncertain_send(order)
            resolved_effect = True
            # The old action is now known to have existed.  Refresh its fill
            # details and, critically, the official remaining position before
            # any caller is allowed to size a new IOC.
            try:
                await self._sync_fills(
                    start_ms=max(int(order.created_ts * 1000) - 10_000, 0)
                )
                await self._sync_position_only()
            except Exception as exc:
                self._pending_ioc_orders[order.cloid] = order
                raise UncertainIocOutcome(
                    f"IOC {order.role} 已找到，但剩余仓位尚未完成官方对账；"
                    "禁止新的 IOC"
                ) from exc
        if resolved_effect:
            raise RuntimeError(
                "上一笔不确定 IOC 已完成官方对账；"
                "本轮不提交新 IOC，等待上层按最新仓位重新计算"
            )

    async def _reconcile_ambiguous_ioc_response(
        self,
        order: TrackedOrder,
        action_row: dict,
        *,
        outcome: str,
    ) -> TrackedOrder:
        """Resolve a malformed/contradictory SDK response without resending."""

        action_row["outcome"] = outcome
        state = await self._resolve_ioc_after_uncertain_send(order)
        action_row["outcome"] = f"reconciled_{state}"
        action_row["oid"] = order.oid
        return order

    async def _submit_reduce_only_ioc(
        self,
        *,
        role: str,
        event: str,
        is_buy: bool,
        qty: float,
        px: float,
    ) -> TrackedOrder:
        """Submit exactly one reduce-only IOC and preserve its fill identity."""

        await self._reconcile_pending_iocs()
        assert self.exchange is not None
        from hyperliquid.utils.types import Cloid

        cloid = self.cloids.next()
        order = TrackedOrder(
            role=role,
            cloid=cloid,
            is_buy=is_buy,
            price=px,
            qty=qty,
            reduce_only=True,
            tif="Ioc",
            created_ts=time.time(),
        )
        request = {
            "coin": self.meta.coin,
            "is_buy": is_buy,
            "sz": qty,
            "limit_px": px,
            "order_type": {"limit": {"tif": "Ioc"}},
            "reduce_only": True,
            "cloid": Cloid.from_str(cloid),
        }
        action_row = self._note_order_event(
            event,
            durable_intent=True,
            role=role,
            cloid=cloid,
            is_buy=is_buy,
            px=px,
            qty=qty,
            reduce_only=True,
            tif="Ioc",
            outcome="submitted",
        )
        self.action_budget.spend(1)
        try:
            response = await self._sdk(self.exchange.bulk_orders, [request])
        except Exception as exc:
            action_row["outcome"] = "send_uncertain"
            try:
                state = await self._resolve_ioc_after_uncertain_send(order)
            except UncertainIocOutcome:
                raise
            action_row["outcome"] = f"reconciled_{state}"
            action_row["oid"] = order.oid
            return order

        top_status = str(response.get("status") or "") if isinstance(response, dict) else ""
        explicit_top_error = self._explicit_exchange_error(response)
        if explicit_top_error:
            action_row["outcome"] = "rejected"
            action_row["reason"] = explicit_top_error[:160]
            self._pending_ioc_orders.pop(order.cloid, None)
            self._proven_ioc_cloids.discard(order.cloid)
            raise RuntimeError(f"IOC {role} 被交易所拒绝: {explicit_top_error[:200]}")
        if not isinstance(response, dict) or top_status != "ok":
            return await self._reconcile_ambiguous_ioc_response(
                order,
                action_row,
                outcome="malformed_top_response",
            )
        response_body = response.get("response")
        response_data = (
            response_body.get("data") if isinstance(response_body, dict) else None
        )
        statuses = (
            response_data.get("statuses") if isinstance(response_data, dict) else None
        )
        if not isinstance(statuses, list) or len(statuses) != 1:
            return await self._reconcile_ambiguous_ioc_response(
                order,
                action_row,
                outcome="malformed_response",
            )

        status = statuses[0]
        if not isinstance(status, dict):
            return await self._reconcile_ambiguous_ioc_response(
                order,
                action_row,
                outcome="malformed_response",
            )

        result_kinds = [
            kind
            for kind in ("filled", "resting")
            if isinstance(status.get(kind), dict)
        ]
        if result_kinds:
            self._mark_ioc_proven(order)
        if set(status) == {"error"} and str(status.get("error") or "").strip():
            action_row["outcome"] = "rejected"
            action_row["reason"] = str(status.get("error"))[:160]
            self._pending_ioc_orders.pop(order.cloid, None)
            self._proven_ioc_cloids.discard(order.cloid)
            raise RuntimeError(
                f"IOC {role} 被交易所拒绝: {str(status.get('error'))[:200]}"
            )
        if len(result_kinds) != 1 or "error" in status:
            return await self._reconcile_ambiguous_ioc_response(
                order,
                action_row,
                outcome="contradictory_response" if result_kinds else "unknown_response",
            )

        result_kind = result_kinds[0]
        result = status[result_kind]

        oid = _positive_order_id(result.get("oid"))
        if oid:
            self._register_ioc_identity(order, oid)
            action_row["oid"] = oid
        if result_kind == "resting":
            action_row["outcome"] = "abnormal_resting"
            state = await self._cancel_abnormal_resting_ioc(order)
            action_row["outcome"] = f"resting_then_{state}"
            return order
        if not oid:
            return await self._reconcile_ambiguous_ioc_response(
                order,
                action_row,
                outcome="filled_without_oid",
            )
        action_row["outcome"] = "filled"
        return order

    async def reduce_position_ioc(
        self, book: OrderBook, qty: float, slippage_bps: float
    ) -> None:
        """Actively shed part of a stuck position with a bounded-slippage IOC."""

        qty = self.meta.qty_floor(min(qty, abs(self.account.position_qty)))
        if qty < self.meta.size_step / 2:
            return
        if book.best_bid is None or book.best_ask is None:
            raise RuntimeError("空盘口，无法主动减仓")
        is_buy = self.account.position_qty < 0
        raw_px = (
            book.best_ask * (1.0 + slippage_bps / 10_000)
            if is_buy
            else book.best_bid * (1.0 - slippage_bps / 10_000)
        )
        px = self.meta.px_round(raw_px, round_up=is_buy)
        log.warning(
            "库存老化主动减仓: %s %.8f @ 限价 %.6f（滑点上限 %.0f bps）",
            "买入回补" if is_buy else "卖出减持",
            qty,
            px,
            slippage_bps,
        )
        await self._submit_reduce_only_ioc(
            role="risk:inventory-ioc",
            event="inventory_ioc",
            is_buy=is_buy,
            qty=qty,
            px=px,
        )

    async def flatten(self, book: OrderBook, slippage_bps: float) -> None:
        qty = self.meta.qty_floor(abs(self.account.position_qty))
        if qty < self.meta.size_step / 2:
            return
        if book.best_bid is None or book.best_ask is None:
            raise RuntimeError("空盘口，无法紧急平仓")
        is_buy = self.account.position_qty < 0
        raw_px = (
            book.best_ask * (1.0 + slippage_bps / 10_000)
            if is_buy
            else book.best_bid * (1.0 - slippage_bps / 10_000)
        )
        px = self.meta.px_round(raw_px, round_up=is_buy)
        order = await self._submit_reduce_only_ioc(
            role="risk:emergency-flatten",
            event="flatten",
            is_buy=is_buy,
            qty=qty,
            px=px,
        )
        # Pull official fill details while the in-memory oid/cloid mapping is
        # alive.  Detail propagation may lag; position reconciliation below is
        # the safety authority and the mapping remains available for recovery.
        try:
            await self._sync_fills(
                start_ms=max(int(order.created_ts * 1000) - 10_000, 0)
            )
        except Exception as exc:
            log.warning("紧急平仓成交明细暂未同步: %s", exc)
        try:
            await self._sync_position_only()
        except Exception as exc:
            raise UncertainIocOutcome(
                "紧急平仓已提交，但无法核实官方剩余仓位；"
                "已保留 cloid/oid 且禁止盲目重发"
            ) from exc
        residual = abs(self.account.position_qty)
        if residual >= self.meta.size_step / 2:
            raise RuntimeError(
                f"紧急平仓后官方仍有剩余仓位 {residual:.8f}；"
                "由停机层按已核实剩余量重试"
            )
