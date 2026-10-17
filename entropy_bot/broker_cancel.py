"""Bounded cancellation using official open-order and fill evidence."""
from __future__ import annotations

from .broker_protocol import CANCELED_ORDER_STATES
from .broker_protocol import CANCEL_MAX_SUBMIT_ATTEMPTS
from .broker_protocol import CANCEL_RETRY_INTERVAL_SEC
from .broker_protocol import FILL_RECONCILIATION_GRACE_SEC
from .broker_protocol import ORDER_RECONCILIATION_GRACE_SEC
from .broker_protocol import PendingCancelReconciliation
from .broker_protocol import PendingFillReconciliation
from .broker_protocol import WS_CANCEL_CONFIRM_SEC
from .broker_protocol import _order_state
from .broker_protocol import _reported_filled_qty
from typing import Iterable
import asyncio
import time
import logging

log = logging.getLogger("entropy.broker")

class LiveCancelMixin:
    """Bounded cancellation using official open-order and fill evidence. Shared state is initialized only by LiveBroker."""

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
