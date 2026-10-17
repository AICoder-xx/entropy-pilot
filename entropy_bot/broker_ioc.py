"""Reduce-only IOC execution with unresolved-identity gates."""
from __future__ import annotations

from .broker_protocol import CANCELED_ORDER_STATES
from .broker_protocol import UncertainIocOutcome
from .broker_protocol import _order_state
from .broker_protocol import _positive_order_id
from .models import OrderBook
from .models import TrackedOrder
import asyncio
import time
import logging

log = logging.getLogger("entropy.broker")

class LiveIocMixin:
    """Reduce-only IOC execution with unresolved-identity gates. Shared state is initialized only by LiveBroker."""

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
            await self._resolve_ioc_after_uncertain_send(order)
            resolved_effect = True
            # The old action is now known to have existed.  Refresh its fill
            # details and, critically, the official remaining position before
            # any caller is allowed to size a new IOC.
            try:
                await self._sync_fills(
                    start_ms=max(int(order.created_ts * 1000) - 10_000, 0)
                )
                await self._sync_position_only()
            except asyncio.CancelledError:
                self._pending_ioc_orders[order.cloid] = order
                raise
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
        def preserve(cloid: str, row: dict) -> None:
            self._pending_ioc_orders[cloid] = order
            if row.get("outcome") == "submitted":
                row["outcome"] = "send_uncertain"

        with self._submission_scope({cloid: action_row}, preserve):
            self.action_budget.spend(1)
            try:
                response = await self._sdk(self.exchange.bulk_orders, [request])
            except Exception:
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
