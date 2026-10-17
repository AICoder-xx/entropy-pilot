"""Public broker API and live-session state. Components share this one state owner."""
from __future__ import annotations

from .broker_account import LiveAccountMixin
from .broker_cancel import LiveCancelMixin
from .broker_execution import LiveExecutionMixin
from .broker_ioc import LiveIocMixin
from .broker_orders import LiveOrdersMixin
from .broker_ws import LiveWsMixin
from .config import Config
from .market import MarketClient
from .models import FeeModel
from .models import MarketMeta
from .models import TrackedOrder
from .pacing import ActionBudget
from .signing_lock import SignerProcessLock
from collections import deque
import asyncio
import time
import logging

from .broker_protocol import BOT_CLOID_MAGIC as BOT_CLOID_MAGIC
from .broker_protocol import CANCELED_ORDER_STATES as CANCELED_ORDER_STATES
from .broker_protocol import CANCEL_MAX_SUBMIT_ATTEMPTS as CANCEL_MAX_SUBMIT_ATTEMPTS
from .broker_protocol import CANCEL_RETRY_INTERVAL_SEC as CANCEL_RETRY_INTERVAL_SEC
from .broker_protocol import CloidFactory as CloidFactory
from .broker_protocol import FILL_RECONCILIATION_GRACE_SEC as FILL_RECONCILIATION_GRACE_SEC
from .broker_protocol import ORDER_RECONCILIATION_GRACE_SEC as ORDER_RECONCILIATION_GRACE_SEC
from .broker_protocol import PendingCancelReconciliation as PendingCancelReconciliation
from .broker_protocol import PendingFillReconciliation as PendingFillReconciliation
from .broker_protocol import PendingOrderReconciliation as PendingOrderReconciliation
from .broker_protocol import RETIRED_ORDER_FILL_GRACE_SEC as RETIRED_ORDER_FILL_GRACE_SEC
from .broker_protocol import STARTUP_EXTERNAL_ACTIVITY_QUIET_SEC as STARTUP_EXTERNAL_ACTIVITY_QUIET_SEC
from .broker_protocol import UncertainIocOutcome as UncertainIocOutcome
from .broker_protocol import WS_CANCEL_CONFIRM_SEC as WS_CANCEL_CONFIRM_SEC
from .broker_protocol import WS_STALE_FALLBACK_SEC as WS_STALE_FALLBACK_SEC
from .broker_protocol import _http_status_code as _http_status_code
from .broker_protocol import _order_state as _order_state
from .broker_protocol import _positive_order_id as _positive_order_id
from .broker_protocol import _reported_filled_qty as _reported_filled_qty
from .broker_protocol import _validated_positions as _validated_positions
from .broker_protocol import allocation_state as allocation_state
from .broker_protocol import is_bot_cloid as is_bot_cloid
from .broker_protocol import order_ownership as order_ownership
from .broker_protocol import recovered_order_identity as recovered_order_identity
from .broker_base import Broker as Broker
from .paper_broker import PaperBroker as PaperBroker


log = logging.getLogger("entropy.broker")


class LiveBroker(LiveWsMixin, LiveExecutionMixin, LiveOrdersMixin, LiveCancelMixin, LiveAccountMixin, LiveIocMixin, Broker):
    """Coordinate isolated execution, feeds and reconciliation components."""

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
        self._signing_lock: SignerProcessLock | None = None
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
        # monotonic() has an arbitrary origin (and can be near zero on a newly
        # booted host). Never treat that origin as a completed official sync.
        self._last_fill_sync = float("-inf")
        self._last_full_fill_sync = float("-inf")
        self._last_account_sync = float("-inf")
        self._last_rest_sweep = float("-inf")
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
