"""ALO placement, atomic modifications and identity reconciliation."""
from __future__ import annotations

from .broker_protocol import CANCELED_ORDER_STATES
from .broker_protocol import _http_status_code
from .broker_protocol import _order_state
from .broker_protocol import _positive_order_id
from .broker_protocol import _reported_filled_qty
from .broker_protocol import allocation_state
from .models import OrderBook
from .models import OrderIntent
from .models import TrackedOrder
import asyncio
import time
import logging

log = logging.getLogger("entropy.broker")

class LiveOrdersMixin:
    """ALO placement, atomic modifications and identity reconciliation. Shared state is initialized only by LiveBroker."""

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
        intents_by_cloid = {cloid: intent for intent, cloid in prepared}

        def preserve(cloid: str, row: dict) -> None:
            if cloid not in self._identity_pending_cloids:
                self._reserve_uncertain_place(intents_by_cloid[cloid], cloid, row)

        with self._submission_scope(action_rows, preserve):
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
        modifications = {cloid: (order, intent) for order, intent, cloid in prepared}

        def preserve(cloid: str, row: dict) -> None:
            if cloid not in self._identity_pending_cloids:
                order, intent = modifications[cloid]
                self._reserve_uncertain_modify(order, intent, cloid, row)

        with self._submission_scope(action_rows, preserve):
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
