"""Durable inventory and grid-pair accounting for one account and market.

All amounts are stored as decimal strings inside an atomic SQLite transaction.
The inventory book uses the exchange's gross moving-average entry convention;
opening fees are expensed immediately.  Pair P&L is an independent attribution
of grid opening fills to subsequent grid closing fills, including both fees.
It must never be added to inventory P&L.

``net_invested_usd`` is a *signed equivalent break-even basis*, not cash paid
for a perpetual contract, margin, or a withdrawable balance.  A short has a
negative signed quantity and usually a negative equivalent basis.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path
from typing import Any


ZERO = Decimal("0")
EPSILON = Decimal("0.000000000001")
SCHEMA_VERSION = 1


class GridAccountingError(RuntimeError):
    """Evidence is insufficient or inconsistent; new grid quotes must stop."""


def _decimal(value: Any, name: str) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise GridAccountingError(f"{name} 缺失或不是有效数值")
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise GridAccountingError(f"{name} 不是有效数值") from exc
    if not number.is_finite():
        raise GridAccountingError(f"{name} 必须为有限数值")
    return number


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _value(fill: Any, key: str, default: Any = None) -> Any:
    return fill.get(key, default) if isinstance(fill, dict) else getattr(fill, key, default)


class GridAccounting:
    """One persistent book; ``identity`` must include the account and market.

    ``initialize`` seeds only a new book from an authoritative position.  On
    reopening an existing book ``reconcile`` is required before ``status`` can
    become ``ready``.  Callers must also prevent simultaneous writers/traders
    for the same account-market and reconcile every official position update.

    A direction change is accepted only at zero inventory with no unmatched
    lots; callers additionally have to prove all exchange orders are gone.
    """

    def __init__(self, path: str | Path, identity: str | dict, direction: str = "long") -> None:
        if direction not in {"long", "short"}:
            raise ValueError("网格方向只能为 long 或 short")
        if not identity:
            raise ValueError("库存账本必须绑定明确的账户与市场标识")
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.identity = _json(identity)
        self.direction = direction
        self._closed = False
        self._needs_reconcile = False
        self._db = sqlite3.connect(str(self.path), timeout=5.0, isolation_level=None)
        try:
            self._db.execute("PRAGMA journal_mode=WAL")
            self._db.execute("PRAGMA synchronous=FULL")
            self._db.execute("PRAGMA busy_timeout=5000")
            self._db.execute(
                "CREATE TABLE IF NOT EXISTS accounting_meta "
                "(key TEXT PRIMARY KEY, value TEXT NOT NULL)"
            )
            self._db.execute(
                "CREATE TABLE IF NOT EXISTS accounting_state "
                "(id INTEGER PRIMARY KEY CHECK(id=1), value TEXT NOT NULL)"
            )
            self._db.execute(
                "CREATE TABLE IF NOT EXISTS accounting_events "
                "(kind TEXT NOT NULL, event_id TEXT NOT NULL, time_ms INTEGER NOT NULL, "
                "digest TEXT NOT NULL, payload TEXT NOT NULL, "
                "PRIMARY KEY(kind,event_id))"
            )
            self._db.execute("BEGIN IMMEDIATE")
            saved = self._db.execute(
                "SELECT value FROM accounting_meta WHERE key='identity'"
            ).fetchone()
            if saved is not None and saved[0] != self.identity:
                raise GridAccountingError("库存账本账户/市场标识不符，禁止复用其他账户账本")
            self._db.execute(
                "INSERT OR IGNORE INTO accounting_meta(key,value) VALUES('identity',?)",
                (self.identity,),
            )
            state = self._load()
            if state is None:
                state = self._empty_state(direction)
                self._save(state)
            elif state.get("schema_version") != SCHEMA_VERSION:
                raise GridAccountingError("库存账本版本不受支持，需要明确迁移")
            if state["direction"] != direction:
                if _decimal(state["qty"], "库存") != 0 or state["lots"]:
                    raise GridAccountingError("非空库存或待配对成交存在，禁止切换网格方向")
                state["direction"] = direction
                self._save(state)
            self._needs_reconcile = bool(state["initialized"])
            self._db.execute("COMMIT")
        except Exception:
            if self._db.in_transaction:
                self._db.execute("ROLLBACK")
            self._db.close()
            self._closed = True
            raise

    @staticmethod
    def _empty_state(direction: str) -> dict:
        return {
            "schema_version": SCHEMA_VERSION, "direction": direction,
            "initialized": False, "inception_time_ms": None,
            "initial_qty": "0", "initial_entry": "0", "qty": "0", "entry": "0",
            "realized": "0", "fees": "0", "funding": "0",
            "pair_gross": "0", "pair_fees": "0", "matched_qty": "0",
            "discarded_pair_qty": "0", "lots": [], "fill_count": 0,
            "funding_count": 0, "last_fill_time_ms": None,
            "funding_complete": False, "funding_coverage_until_ms": None,
            "fatal_reason": None, "reconciliation_reason": None,
        }

    def _load(self) -> dict | None:
        row = self._db.execute("SELECT value FROM accounting_state WHERE id=1").fetchone()
        return json.loads(row[0]) if row else None

    def _save(self, state: dict) -> None:
        self._db.execute(
            "INSERT INTO accounting_state(id,value) VALUES(1,?) "
            "ON CONFLICT(id) DO UPDATE SET value=excluded.value", (_json(state),),
        )

    def _state(self) -> dict:
        state = self._load()
        if state is None:
            raise GridAccountingError("库存账本状态缺失")
        return state

    def _fail(self, state: dict, reason: str) -> None:
        state["fatal_reason"] = reason
        self._save(state)
        self._db.execute("COMMIT")
        raise GridAccountingError(reason)

    def initialize(self, qty: Any, entry: Any, time_ms: int | None = None) -> bool:
        """Seed verified opening inventory without manufacturing past profit."""
        self._db.execute("BEGIN IMMEDIATE")
        try:
            state = self._state()
            if state["initialized"]:
                self._db.execute("COMMIT")
                return False
            amount = _decimal(qty, "启动持仓数量")
            price = _decimal(entry, "启动持仓成本")
            sign = 1 if self.direction == "long" else -1
            # No economic event has been accepted yet. Invalid bootstrap
            # inputs must roll back so a corrected direction/cost can retry;
            # only inconsistencies in an established book become sticky.
            if amount * sign < 0:
                raise GridAccountingError("官方启动持仓方向与所选网格方向不符")
            if amount != 0 and price <= 0:
                raise GridAccountingError("官方启动持仓成本未知，不能把库存成本当作零")
            timestamp = _decimal(time_ms if time_ms is not None else time.time() * 1000, "开始记账时间")
            if timestamp < 0:
                raise GridAccountingError("开始记账时间无效")
            inception = int(timestamp)
            state.update({
                "initialized": True, "qty": str(amount),
                "entry": str(price if amount else ZERO),
                "initial_qty": str(amount), "initial_entry": str(price if amount else ZERO),
                "inception_time_ms": inception,
            })
            self._save(state)
            self._db.execute("COMMIT")
            self._needs_reconcile = False
            return True
        except Exception:
            if self._db.in_transaction:
                self._db.execute("ROLLBACK")
            raise

    def _existing(self, kind: str, event_id: str, digest: str, state: dict) -> bool:
        row = self._db.execute(
            "SELECT digest FROM accounting_events WHERE kind=? AND event_id=?",
            (kind, event_id),
        ).fetchone()
        if row is None:
            return False
        if row[0] != digest:
            self._fail(state, f"同一{kind}标识出现矛盾内容，需要人工核对官方成交")
        return True

    def has_fill(self, fill_id: str) -> bool:
        """Read-only replay helper; does not claim the replay payload matches.

        Runtime may use this before validating the start-position chain for an
        entire pending batch. ``record_fill`` still checks duplicate contents
        when a caller wants to validate an already-booked economic event.
        """
        return self._db.execute(
            "SELECT 1 FROM accounting_events WHERE kind='fill' AND event_id=?",
            (str(fill_id),),
        ).fetchone() is not None

    def record_fill(self, fill: Any) -> bool:
        """Apply a Fill once; attribution is grid opening -> grid closing FIFO.

        A baseline-position close generates real inventory P&L but no grid-pair
        P&L.  Non-grid reductions never create pair profit and retire any lots
        which could no longer be backed by the remaining real inventory.
        """
        self._db.execute("BEGIN IMMEDIATE")
        try:
            state = self._state()
            if not state["initialized"]:
                self._fail(state, "库存账本尚未从官方持仓初始化")
            event_id = str(_value(fill, "fill_id", ""))
            if not event_id:
                self._fail(state, "成交缺少唯一标识，不能幂等记账")
            try:
                qty = _decimal(_value(fill, "qty"), "成交数量")
                px = _decimal(_value(fill, "price"), "成交价")
                fee = _decimal(_value(fill, "fee_usd"), "成交手续费")
                timestamp = int(_value(fill, "time_ms"))
                is_buy = _value(fill, "is_buy")
                if not isinstance(is_buy, bool) or qty <= 0 or px <= 0 or timestamp < 0:
                    raise GridAccountingError("成交方向、数量、价格或时间无效")
                start_raw = _value(fill, "start_position_qty")
                start = _decimal(start_raw, "官方成交前仓位") if start_raw is not None else None
            except (ValueError, TypeError, GridAccountingError) as exc:
                self._fail(state, str(exc))
            role = str(_value(fill, "order_role", ""))
            payload = {
                "fill_id": event_id, "qty": str(qty.normalize()), "price": str(px.normalize()),
                "fee_usd": str(fee.normalize()), "time_ms": timestamp, "is_buy": is_buy,
                "order_role": role, "fee_known": bool(_value(fill, "fee_known", True)),
                "fee_token": _value(fill, "fee_token", "USDC"),
                "realized_pnl_known": bool(_value(fill, "realized_pnl_known", True)),
                "start_position_qty": str(start.normalize()) if start is not None else None,
            }
            encoded = _json(payload)
            # REST recovery may know less order-role metadata than the live
            # broker. Identity is the economic event, not its discovery path.
            economic = {key: value for key, value in payload.items() if key not in {"order_role", "start_position_qty"}}
            digest = hashlib.sha256(_json(economic).encode("utf-8")).hexdigest()
            if self._existing("fill", event_id, digest, state):
                self._db.execute("COMMIT")
                return False
            if state["fatal_reason"]:
                self._fail(state, state["fatal_reason"])
            if not payload["fee_known"] or payload["fee_token"] != "USDC":
                self._fail(state, "成交手续费缺失或不是已核实的 USDC 金额，真实净盈亏未知")
            if not payload["realized_pnl_known"]:
                self._fail(state, "官方成交已实现盈亏缺失，成交证据不完整")
            if timestamp < state["inception_time_ms"]:
                self._db.execute("COMMIT")
                return False
            if state["last_fill_time_ms"] is not None and timestamp < state["last_fill_time_ms"]:
                self._fail(state, "成交乱序到达，必须按官方顺序重建成本后才能继续报价")
            current = _decimal(state["qty"], "库存")
            if start is not None and abs(start - current) > EPSILON:
                self._fail(state, "官方成交前仓位与库存账本不符，可能缺少成交或存在外部交易")
            with localcontext() as context:
                context.prec = 38
                self._apply_fill(state, qty, px, fee, is_buy, role, event_id)
            state["last_fill_time_ms"] = timestamp
            state["fill_count"] += 1
            self._db.execute(
                "INSERT INTO accounting_events(kind,event_id,time_ms,digest,payload) "
                "VALUES('fill',?,?,?,?)", (event_id, timestamp, digest, encoded),
            )
            self._save(state)
            self._db.execute("COMMIT")
            return True
        except Exception:
            if self._db.in_transaction:
                self._db.execute("ROLLBACK")
            raise

    def _apply_fill(
        self, state: dict, qty: Decimal, px: Decimal, fee: Decimal,
        is_buy: bool, role: str, event_id: str,
    ) -> None:
        sign = Decimal(1 if state["direction"] == "long" else -1)
        previous = abs(_decimal(state["qty"], "库存"))
        entry = _decimal(state["entry"], "库存成本")
        opening = is_buy == (sign > 0)
        is_grid = role.startswith("grid:")
        if opening:
            amount = previous + qty
            state["qty"] = str(sign * amount)
            state["entry"] = str((previous * entry + qty * px) / amount)
            if is_grid:
                state["lots"].append({"fill_id": event_id, "qty": str(qty), "price": str(px), "fee": str(fee)})
        else:
            if qty - previous > EPSILON:
                self._fail(state, "减仓成交超过真实库存，禁止把反向开仓当成网格平仓")
            closing = min(qty, previous)
            amount = max(previous - closing, ZERO)
            state["qty"] = str(sign * amount if amount else ZERO)
            state["entry"] = str(entry if amount else ZERO)
            state["realized"] = str(_decimal(state["realized"], "真实已实现") + closing * (px - entry) * sign)
            if is_grid:
                remaining = closing
                kept = []
                for lot in state["lots"]:
                    lot_qty = _decimal(lot["qty"], "待配对数量")
                    matched = min(lot_qty, remaining)
                    if matched > 0:
                        opening_fee = _decimal(lot["fee"], "配对开仓费") * matched / lot_qty
                        closing_fee = fee * matched / qty
                        gross = matched * (px - _decimal(lot["price"], "配对开仓价")) * sign
                        state["pair_gross"] = str(_decimal(state["pair_gross"], "配对毛利") + gross)
                        state["pair_fees"] = str(_decimal(state["pair_fees"], "配对费用") + opening_fee + closing_fee)
                        state["matched_qty"] = str(_decimal(state["matched_qty"], "配对数量") + matched)
                        lot["fee"] = str(_decimal(lot["fee"], "待配对费用") - opening_fee)
                        lot["qty"] = str(lot_qty - matched)
                        remaining -= matched
                    if _decimal(lot["qty"], "剩余待配对数量") > 0:
                        kept.append(lot)
                state["lots"] = kept
            # Non-grid exits consume baseline inventory first.  They retire
            # excess grid lots without assigning an imaginary grid profit.
            unmatched = sum((_decimal(lot["qty"], "待配对数量") for lot in state["lots"]), ZERO)
            discard = max(unmatched - amount, ZERO)
            if discard:
                state["discarded_pair_qty"] = str(_decimal(state["discarded_pair_qty"], "未归因数量") + discard)
                kept = []
                for lot in state["lots"]:
                    lot_qty = _decimal(lot["qty"], "待配对数量")
                    removed = min(lot_qty, discard)
                    remaining = lot_qty - removed
                    discard -= removed
                    if remaining > 0:
                        lot["fee"] = str(_decimal(lot["fee"], "待配对费用") * remaining / lot_qty)
                        lot["qty"] = str(remaining)
                        kept.append(lot)
                state["lots"] = kept
        state["fees"] = str(_decimal(state["fees"], "手续费") + fee)

    def record_funding(self, event_id: str, time_ms: int, amount: Any) -> bool:
        """Record signed USDC funding cashflow; positive means received."""
        self._db.execute("BEGIN IMMEDIATE")
        try:
            state = self._state()
            if not state["initialized"]:
                self._fail(state, "记录资金费用前必须初始化库存")
            try:
                value = _decimal(amount, "资金费用")
                timestamp = int(time_ms)
                if not event_id or timestamp < 0:
                    raise GridAccountingError("资金费用缺少唯一标识或有效时间")
            except (ValueError, TypeError, GridAccountingError) as exc:
                self._fail(state, str(exc))
            if timestamp < state["inception_time_ms"]:
                self._db.execute("COMMIT")
                return False
            payload = _json({"event_id": str(event_id), "time_ms": timestamp, "amount": str(value.normalize())})
            digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
            if self._existing("funding", str(event_id), digest, state):
                self._db.execute("COMMIT")
                return False
            state["funding"] = str(_decimal(state["funding"], "资金费用合计") + value)
            state["funding_count"] += 1
            self._db.execute(
                "INSERT INTO accounting_events(kind,event_id,time_ms,digest,payload) "
                "VALUES('funding',?,?,?,?)", (str(event_id), timestamp, digest, payload),
            )
            self._save(state)
            self._db.execute("COMMIT")
            return True
        except Exception:
            if self._db.in_transaction:
                self._db.execute("ROLLBACK")
            raise

    def mark_funding_coverage(self, until_ms: int, *, complete: bool = True) -> None:
        """Call only after a complete official query from the inception time."""
        self._db.execute("BEGIN IMMEDIATE")
        try:
            state = self._state()
            until = int(until_ms)
            if not state["initialized"] or until < state["inception_time_ms"]:
                raise GridAccountingError("资金费用查询覆盖范围早于记账起点")
            state["funding_complete"] = bool(complete)
            state["funding_coverage_until_ms"] = until if complete else None
            self._save(state)
            self._db.execute("COMMIT")
        except Exception:
            if self._db.in_transaction:
                self._db.execute("ROLLBACK")
            raise

    def reconcile(self, position_qty: Any, entry_px: Any, tolerance: Any = "0.00000001") -> bool:
        """Compare official signed inventory and cost; never overwrite history."""
        self._db.execute("BEGIN IMMEDIATE")
        try:
            state = self._state()
            qty = _decimal(position_qty, "官方仓位")
            entry = _decimal(entry_px, "官方持仓成本")
            tol = _decimal(tolerance, "仓位对账容差")
            if tol < 0:
                raise GridAccountingError("仓位对账容差不能为负数")
            reason = None
            if not state["initialized"]:
                reason = "库存账本尚未初始化"
            elif abs(qty - _decimal(state["qty"], "库存")) > tol:
                reason = "官方仓位与库存账本不符：存在遗漏成交、外部交易或转入转出，暂停报价"
            elif qty != 0:
                actual_entry = _decimal(state["entry"], "库存成本")
                # entryPx may be rounded by the official account view.  The
                # tolerance is relative in price, independent of lot size.
                price_tol = max(Decimal("0.000001"), abs(actual_entry) * Decimal("0.00000001"))
                if entry <= 0 or abs(entry - actual_entry) > price_tol:
                    reason = "官方持仓成本与库存账本不符，不能自动重置成本掩盖盈亏"
            state["reconciliation_reason"] = reason
            self._save(state)
            self._db.execute("COMMIT")
            self._needs_reconcile = False
            return not reason and not state["fatal_reason"]
        except Exception:
            if self._db.in_transaction:
                self._db.execute("ROLLBACK")
            self._needs_reconcile = True
            raise

    def snapshot(self, mark_px: Any = None) -> dict:
        state = self._state()
        with localcontext() as context:
            context.prec = 38
            qty = _decimal(state["qty"], "库存")
            entry = _decimal(state["entry"], "成本")
            realized = _decimal(state["realized"], "真实毛利")
            fees = _decimal(state["fees"], "费用")
            funding = _decimal(state["funding"], "资金费用")
            pair_gross = _decimal(state["pair_gross"], "配对毛利")
            pair_fees = _decimal(state["pair_fees"], "配对费用")
            unmatched = sum((_decimal(lot["qty"], "待配对数量") for lot in state["lots"]), ZERO)
            mark = None if mark_px is None else _decimal(mark_px, "标记价格")
            if mark is not None and mark <= 0:
                raise GridAccountingError("标记价格必须为正数")
            unrealized = qty * (mark - entry) if mark is not None else (ZERO if qty == 0 else None)
            trade_net = realized - fees
            net = trade_net + funding
            basis = qty * entry - net
            funding_complete = bool(state["funding_complete"])
            status, reason = "ready", None
            if not state["initialized"]:
                status, reason = "uninitialized", "尚未从官方持仓初始化"
            if self._needs_reconcile or state["reconciliation_reason"]:
                status, reason = "reconciliation_required", state["reconciliation_reason"] or "重启后必须先核对官方仓位与成本"
            if state["fatal_reason"]:
                status, reason = "error", state["fatal_reason"]
            verified = status == "ready"
            full_net_known = verified and funding_complete
            number = lambda value: float(value) if value is not None else None
            return {
                "schema_version": SCHEMA_VERSION, "direction": state["direction"],
                "status": status, "reason": reason, "initialized": state["initialized"],
                "inventory_qty": number(qty), "average_entry_price": number(entry),
                "inventory_cost_usd": number(abs(qty) * entry),
                "realized_gross_usd": number(realized), "fees_paid_usd": number(fees),
                "funding_net_usd": number(funding),
                "realized_after_trade_fees_usd": number(trade_net) if verified else None,
                "realized_net_usd": number(net) if full_net_known else None,
                "unrealized_usd": number(unrealized) if verified else None,
                "total_after_trade_fees_usd": number(trade_net + unrealized) if verified and unrealized is not None else None,
                "total_net_usd": number(net + unrealized) if full_net_known and unrealized is not None else None,
                "grid_pair_gross_usd": number(pair_gross), "grid_pair_fees_usd": number(pair_fees),
                "grid_pair_net_usd": number(pair_gross - pair_fees) if verified else None,
                "matched_qty": number(_decimal(state["matched_qty"], "配对数量")),
                "unmatched_qty": number(unmatched),
                "unmatched_open_fees_usd": number(sum((_decimal(lot["fee"], "待配对费用") for lot in state["lots"]), ZERO)),
                "discarded_pair_qty": number(_decimal(state["discarded_pair_qty"], "非网格减仓数量")),
                "net_invested_usd": number(basis) if full_net_known else None,
                "break_even_price": number(basis / qty) if full_net_known and qty else None,
                "inception_time_ms": state["inception_time_ms"],
                "last_fill_time_ms": state["last_fill_time_ms"],
                "initial_inventory_qty": number(_decimal(state["initial_qty"], "期初库存")),
                "initial_entry_price": number(_decimal(state["initial_entry"], "期初成本")),
                "fill_count": state["fill_count"], "funding_event_count": state["funding_count"],
                "history_scope": "since_inception", "historical_fees_included": False,
                "historical_funding_included": False, "projected_rebates_included": False,
                "funding_complete": funding_complete,
                "funding_coverage_since_ms": state["inception_time_ms"],
                "funding_coverage_until_ms": state["funding_coverage_until_ms"],
                "net_profit_complete": full_net_known,
                "cost_basis_method": "gross_moving_average",
                "pairing_method": "grid_open_to_close_fifo",
                "net_invested_semantics": "signed_equivalent_break_even_basis_not_margin_or_withdrawable_cash",
            }

    def close(self) -> None:
        if not self._closed:
            self._db.close()
            self._closed = True

    def __enter__(self) -> "GridAccounting":
        return self

    def __exit__(self, *_args: Any) -> None:
        self.close()
