"""Read-only exchange reconciliation around the persistent grid inventory ledger."""
from __future__ import annotations

import json
import math
import time
from itertools import groupby

from .broker import _order_state, _reported_filled_qty, CANCELED_ORDER_STATES, recovered_order_identity
from .storage import order_intent_is_pending

from .grid_accounting import GridAccounting
from .models import Fill


class GridRuntime:
    def __init__(self, cfg, ledger, market, broker, identity: str):
        self.cfg, self.ledger, self.market, self.broker = cfg, ledger, market, broker
        self.book = GridAccounting(
            ledger.state_path.with_name(ledger.slug + "_inventory.sqlite3"),
            identity,
            direction=cfg.grid.inventory_direction,
        )
        self.last_audit = 0.0
        self.reason: str | None = None
        self.roles: dict[str, str] = {}
        self._pending_intents: dict[str, dict] = {}
        self._recovered_expected_fills: dict[int, float] = {}
        self._recovered_intent_outcomes: list[dict] = []
        # Only this grid's durable order identities may earn grid-pair PnL.
        # Unclassified/external fills still change the actual inventory cost.
        for path in (ledger.orders_path, ledger.fills_path):
            if not path.exists():
                continue
            with path.open(encoding="utf-8") as stream:
                for line in stream:
                    try:
                        row = json.loads(line)
                    except (ValueError, TypeError):
                        continue
                    self.remember(row)
        if ledger.state_path.exists():
            try:
                saved = json.loads(ledger.state_path.read_text(encoding="utf-8"))
                for role, row in (saved.get("orders") or {}).items():
                    self.remember({**row, "role": role})
            except (ValueError, OSError):
                pass

    def remember(self, row: dict) -> None:
        cloid = row.get("cloid")
        if cloid and row.get("event") in {"place", "modify", "inventory_ioc", "flatten"}:
            if order_intent_is_pending(row):
                self._pending_intents[str(cloid)] = dict(row)
            elif row.get("outcome"):
                self._pending_intents.pop(str(cloid), None)
        role = row.get("order_role") or row.get("role")
        if not isinstance(role, str) or not role.startswith("grid:"):
            return
        for key in ("cloid", "oid"):
            if row.get(key) is not None:
                self.roles[str(row[key])] = role

    @staticmethod
    def _snapshot_time(payload: dict) -> int:
        timestamp = payload.get("time")
        if isinstance(timestamp, bool) or not isinstance(timestamp, int) or timestamp <= 0:
            raise ValueError("官方仓位快照缺少时间边界，不能安全初始化库存")
        return timestamp

    @staticmethod
    def _position(payload: dict, coin: str) -> tuple[float, float]:
        if not isinstance(payload, dict) or not isinstance(payload.get("assetPositions"), list):
            raise ValueError("官方仓位响应缺少 assetPositions，不能作为空仓")
        for item in payload["assetPositions"]:
            row = item.get("position") or {}
            if row.get("coin") == coin:
                qty = float(row["szi"])
                entry = float(row["entryPx"]) if qty else 0.0
                if not math.isfinite(qty) or not math.isfinite(entry) or (qty and entry <= 0):
                    raise ValueError("官方库存数量/均价无效")
                return qty, entry
        return 0.0, 0.0

    async def initialize(self) -> None:
        if self.cfg.mode == "live":
            # This runs before creating the signer / changing leverage.
            payload = await self.market.account_state(self.broker.address)
            qty, entry = self._position(payload, self.broker.meta.coin)
            snapshot_time = self._snapshot_time(payload)
            self.book.initialize(qty, entry, time_ms=int(snapshot_time))
            # An absent order is not proof that a pre-crash submission failed.
            # Resolve its original client ID before constructing a new signer.
            for cloid, intent in list(self._pending_intents.items()):
                status = await self.market.order_status(self.broker.address, cloid)
                terminal = _order_state(status)
                oid = recovered_order_identity(status, cloid, self.broker.meta.coin)
                if oid is None or terminal not in {"open", "filled", "triggered", *CANCELED_ORDER_STATES}:
                    self.reason = "存在崩溃前未决下单意图，官方仍未证明结果；保持 UNKNOWN，禁止重复下单"
                    return
                self.remember({**intent, "oid": oid})
                self._recovered_expected_fills[oid] = _reported_filled_qty(status)
                self._recovered_intent_outcomes.append({**intent, "oid": oid, "outcome": "recovery_confirmed", "exchange_state": terminal})
            await self.audit(force=True)
        else:
            self.book.initialize(0, 0, time_ms=int(time.time() * 1000))
            state = self.book.snapshot()
            # The simulated broker is restored from its sole durable inventory
            # source; it has no separate exchange to overwrite that source.
            self.book.reconcile(state["inventory_qty"], state["average_entry_price"])
            state = self.book.snapshot()
            account = self.broker.account
            account.position_qty = state["inventory_qty"]
            account.entry_px = state["average_entry_price"] or 0.0
            realized = state.get("realized_after_trade_fees_usd")
            if realized is None:
                raise RuntimeError("模拟网格库存账本不完整，不能重置后继续")
            self.broker._initial_equity = self.cfg.paper.initial_equity_usd + realized
            account.equity_actual = self.broker._initial_equity
            self.book.mark_funding_coverage(int(time.time() * 1000))

    def _raw_fill(self, row: dict) -> Fill:
        def number(name: str):
            raw = row.get(name)
            if raw is None or isinstance(raw, bool) or raw == "":
                return None
            try:
                value = float(raw)
            except (TypeError, ValueError):
                return None
            return value if math.isfinite(value) else None

        if str(row.get("side", "")).upper() not in {"B", "A"}:
            raise ValueError("官方成交方向未知")
        # Official tid must be present. Hashing incomplete fields can silently
        # merge two equal-sized partial executions into one.
        if row.get("tid") is None:
            raise ValueError("官方成交缺少 tid，不能安全去重")
        token = str(row.get("feeToken") or "").strip() or None
        fee, pnl = number("fee"), number("closedPnl")
        role = self.roles.get(str(row.get("cloid"))) or self.roles.get(str(row.get("oid")))
        return Fill(
            fill_id=str(row["tid"]), order_role=role or "external:unclassified",
            cloid=str(row.get("cloid") or ""), oid=row.get("oid"),
            is_buy=str(row["side"]).upper() == "B", price=float(row["px"]),
            qty=float(row["sz"]), fee_usd=fee if fee is not None else 0.0,
            liquidity="taker" if row.get("crossed") else "maker", time_ms=int(row["time"]),
            realized_pnl_delta=pnl if pnl is not None else 0.0,
            start_position_qty=number("startPosition"), fee_token=token,
            fee_known=fee is not None and token == "USDC", realized_pnl_known=pnl is not None,
        )

    def record_paper_fill(self, fill: Fill) -> None:
        self.book.record_fill(fill)
        self.book.mark_funding_coverage(int(time.time() * 1000))

    async def audit(self, *, force: bool = False) -> bool:
        if self.cfg.mode != "live":
            account = self.broker.account
            return self.book.reconcile(account.position_qty, account.entry_px, self.broker.meta.size_step / 2)
        if not force and time.monotonic() - self.last_audit < 15:
            return self.ready()
        try:
            state = self.book.snapshot()
            inception = int(state["inception_time_ms"])
            # Match the broker's five-minute delayed-fill reconciliation window.
            start = max(inception, int(state.get("last_fill_time_ms") or inception) - 310_000)
            if self._pending_intents:
                start = max(inception, min(start, min(int(row.get("time_ms") or inception) for row in self._pending_intents.values()) - 1000))
            # Exchange timestamps can lead the local machine by >1 second.
            # Bound both histories with an official snapshot, never host time.
            before = await self.market.account_state(self.broker.address)
            until = self._snapshot_time(before)
            if until < inception:
                self.reason = "官方仓位时间早于已保存记账边界，等待较新快照"
                return False
            rows = await self.market.fills_since(self.broker.address, start, end_ms=until)
            for oid, expected in self._recovered_expected_fills.items():
                seen = sum(float(row["sz"]) for row in rows
                           if row.get("coin") == self.broker.meta.coin and int(row.get("oid") or 0) == oid)
                if seen + self.broker.meta.size_step / 2 < expected:
                    self.reason = "崩溃前订单已确认成交，但完整明细尚未到齐；暂停新单"
                    return False
            for order in self.broker.orders_by_role.values():
                self.remember(order.to_dict())
            for row in rows:
                if row.get("coin") != self.broker.meta.coin or int(row["time"]) <= inception:
                    continue
                if self.book.has_fill(str(row.get("tid"))):
                    continue
                if str(row.get("oid")) in self.roles or str(row.get("cloid")) in self.roles:
                    continue
                # A crash after send but before the acknowledgement can leave
                # only the durable cloid intent. Resolve the official oid to it.
                status = await self.market.order_status(self.broker.address, int(row["oid"]))
                if not isinstance(status, dict) or status.get("status") != "order":
                    self.reason = "成交订单归属尚未确认，等待官方订单状态；未假定为外部成交"
                    return False
                inner = (status.get("order") or {}).get("order") or {}
                cloid = str(inner.get("cloid") or "")
                if cloid in self.roles:
                    row["cloid"] = cloid
                    self.roles[str(row["oid"])] = self.roles[cloid]
            rows = sorted(rows, key=lambda r: int(r["time"]))
            pending = [self._raw_fill(row) for row in rows
                       if row.get("coin") == self.broker.meta.coin and int(row["time"]) > inception]
            current = float(state["inventory_qty"])
            ordered = [fill for fill in pending if self.book.has_fill(fill.fill_id)]
            fresh = [fill for fill in pending if not self.book.has_fill(fill.fill_id)]
            for timestamp, group in groupby(fresh, key=lambda f: f.time_ms):
                remaining = list(group)
                if state.get("last_fill_time_ms") and timestamp < state["last_fill_time_ms"]:
                    self.reason = "发现早于已入账记录的新成交，需要按完整历史重建；已暂停报价"
                    return False
                while remaining:
                    matches = [f for f in remaining if f.start_position_qty is not None
                               and abs(f.start_position_qty - current) < 1e-10]
                    if len(matches) != 1:
                        self.reason = "官方成交前仓位链缺失或同毫秒顺序有歧义；等待完整成交，未修改库存账本"
                        return False
                    fill = matches[0]
                    ordered.append(fill)
                    remaining.remove(fill)
                    current += fill.qty if fill.is_buy else -fill.qty
            for fill in ordered:
                self.book.record_fill(fill)
            # Fetch after fills. A concurrently arriving fill causes a mismatch
            # and pauses quoting until a subsequent complete audit reconciles it.
            payload = await self.market.account_state(self.broker.address)
            qty, entry = self._position(payload, self.broker.meta.coin)
            matched = self.book.reconcile(qty, entry, self.broker.meta.size_step / 2)
            funding_start = max(inception, int(state.get("funding_coverage_until_ms") or inception) - 310_000)
            try:
                funding = await self.market.user_funding_since(self.broker.address, funding_start, end_ms=until)
                for row in funding:
                    delta = row.get("delta") or {}
                    if delta.get("coin") != self.broker.meta.coin or int(row["time"]) <= inception:
                        continue
                    event_id = f"{row['time']}:{row.get('hash', '')}:{delta['coin']}"
                    self.book.record_funding(event_id, int(row["time"]), delta["usdc"])
                self.book.mark_funding_coverage(until)
            except Exception:
                self.book.mark_funding_coverage(until, complete=False)
            self.reason = None if matched else "官方库存与持久账本尚未一致，等待成交明细补齐"
            self.last_audit = time.monotonic()
            if matched:
                for row in self._recovered_intent_outcomes:
                    self.ledger.record_order_intent(row)
                self._recovered_intent_outcomes.clear()
                self._recovered_expected_fills.clear()
                self._pending_intents.clear()
            return matched and self.ready()
        except Exception as exc:
            # Keep the journal. Network errors/partial history are never zero.
            self.reason = f"库存对账未完成（{type(exc).__name__}）；保留账本，暂停新增挂单"
            return False

    def ready(self) -> bool:
        return self.reason is None and self.book.snapshot().get("status") == "ready"

    def snapshot(self, mark: float | None = None) -> dict:
        value = self.book.snapshot(mark)
        if self.reason:
            value.update(status="unknown", reason=self.reason)
            for field in ("realized_net_usd", "unrealized_usd", "total_net_usd", "break_even_price", "net_invested_usd", "grid_pair_net_usd", "realized_after_trade_fees_usd", "total_after_trade_fees_usd"):
                value[field] = None
            value["net_profit_complete"] = False
        return value

    def close(self) -> None:
        self.book.close()
