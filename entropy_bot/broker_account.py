"""Authoritative account snapshots, fill accounting and reconciliation."""
from __future__ import annotations

from .broker_protocol import CANCELED_ORDER_STATES
from .broker_protocol import FILL_RECONCILIATION_GRACE_SEC
from .broker_protocol import ORDER_RECONCILIATION_GRACE_SEC
from .broker_protocol import PendingFillReconciliation
from .broker_protocol import PendingOrderReconciliation
from .broker_protocol import _order_state
from .broker_protocol import _reported_filled_qty
from .broker_protocol import _validated_positions
from .models import Fill
from .models import OrderBook
from .models import TrackedOrder
from dataclasses import replace
import asyncio
import math
import time
import logging

log = logging.getLogger("entropy.broker")

class LiveAccountMixin:
    """Authoritative account snapshots, fill accounting and reconciliation. Shared state is initialized only by LiveBroker."""

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
