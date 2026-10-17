from __future__ import annotations

import json
import logging
import os
import tempfile
import time
from dataclasses import asdict
from logging.handlers import RotatingFileHandler
from pathlib import Path

from .models import AccountState, FeeModel, Fill, MarketSnapshot


ORDER_INTENT_EVENTS = {"place", "modify", "inventory_ioc", "flatten"}
ORDER_INTENT_FINAL_OUTCOMES = {
    "resting", "filled", "rejected", "rate_limited", "recovery_confirmed",
    "reconciled_open", "reconciled_filled", "reconciled_triggered",
    "reconciled_canceled", "reconciled_cancelled", "reconciled_rejected",
    "resting_then_filled", "resting_then_canceled", "resting_then_cancelled",
    "resting_then_rejected",
}


class OrderIntentJournalError(RuntimeError):
    """Durable identity evidence is damaged; absence cannot authorize a send."""
    code = "ORDER_STATE_UNCERTAIN"


def order_intent_is_pending(row: dict) -> bool:
    """Unknown/malformed outcomes never constitute a submission failure."""
    outcome = str(row.get("outcome") or "")
    if outcome in {"rejected", "rate_limited"}:
        return False
    if outcome not in ORDER_INTENT_FINAL_OUTCOMES:
        return True
    oid = row.get("oid")
    try:
        valid_oid = not isinstance(oid, bool) and int(oid) > 0 and str(int(oid)) == str(oid)
    except (TypeError, ValueError, OverflowError):
        valid_oid = False
    # A nominal success without numeric identity still leaves the broker's
    # identity guard active; a restart must preserve exactly the same guard.
    return not valid_oid


def read_json(path: Path, default=None, *, strict: bool = False):
    if not path.is_file():
        return default
    for attempt in range(6):
        try:
            with path.open("r", encoding="utf-8") as f:
                return json.load(f)
        except (OSError, json.JSONDecodeError):
            if attempt >= 5:
                if strict:
                    raise
                return default
            # The dashboard atomically replaces this file every two seconds.
            # Windows scanners can briefly deny a concurrent open even though
            # the destination is valid. Do not turn that transient lock into a
            # live-trading stop request.
            time.sleep(0.01 * (2**attempt))
    return default


def atomic_write_json(path: Path, data: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".tmp_", suffix=".json")
    tmp_path = Path(tmp)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        for attempt in range(6):
            try:
                os.replace(tmp_path, path)
                break
            except PermissionError:
                if attempt >= 5:
                    raise
                # Windows Defender/indexers can briefly open the destination
                # without FILE_SHARE_DELETE. Keep the original file intact and
                # retry the same prepared temp file with a bounded backoff.
                time.sleep(0.02 * (2**attempt))
        tmp_path = None  # type: ignore[assignment]
    finally:
        if tmp_path is not None and tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass


def append_jsonl(path: Path, data: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(data, ensure_ascii=False, separators=(",", ":")) + "\n")
        f.flush()


def setup_logging(log_dir: Path, verbose: bool = False) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(logging.DEBUG if verbose else logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s | %(message)s", "%Y-%m-%d %H:%M:%S"
    )
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    file_handler = RotatingFileHandler(
        log_dir / "entropy_bot.log",
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    root.handlers.clear()
    root.addHandler(console)
    root.addHandler(file_handler)


class Ledger:
    def __init__(
        self,
        state_dir: Path,
        mode: str,
        strategy: str,
        coin: str,
        session_id: str,
    ) -> None:
        slug = f"{mode}_{strategy}_{coin.replace(':', '_')}"
        self.slug = slug
        self.mode = mode
        self.coin = coin
        self.session_id = session_id
        self.state_path = state_dir / f"{slug}_state.json"
        self.summary_path = state_dir / f"{slug}_summary.json"
        self.fills_path = state_dir / f"{slug}_fills.jsonl"
        self.markouts_path = state_dir / f"{slug}_markouts.jsonl"
        self.orders_path = state_dir / f"{slug}_orders.jsonl"
        self.fill_count = 0
        self._last_saved_state: dict | None = None

    def load_strategy_state(self) -> dict:
        data = read_json(self.state_path, {}, strict=False)
        return data.get("strategy") if isinstance(data, dict) else {}

    def save_state(self, strategy_state: dict, orders: dict) -> None:
        payload = {
            "strategy": strategy_state,
            "orders": {role: order.to_dict() for role, order in orders.items()},
        }
        if payload == self._last_saved_state and self.state_path.exists():
            return
        atomic_write_json(self.state_path, payload)
        # Nested strategy dictionaries can mutate between loops.
        self._last_saved_state = json.loads(json.dumps(payload))

    def record_fill(
        self,
        fill: Fill,
        mode: str,
        strategy: str,
        coin: str,
        *,
        mid_at_record: float | None,
        account_equity_after: float,
    ) -> None:
        self.fill_count += 1
        row = asdict(fill)
        projected_rebate = (
            fill.projected_self_rebate + fill.projected_referred_benefit
        )
        projected_net_fee = fill.fee_usd - projected_rebate
        row.update(
            {
                "session_id": self.session_id,
                "mode": mode,
                "strategy": strategy,
                "coin": coin,
                "notional_usd": fill.notional,
                "mid_at_record": mid_at_record,
                "account_equity_after": account_equity_after,
                "projected_rebate_total_usd": projected_rebate,
                "projected_net_fee_usd": projected_net_fee if fill.fee_known else None,
                "actual_booked_wear_usd": (
                    fill.fee_usd - fill.realized_pnl_delta if fill.fee_known and fill.realized_pnl_known else None
                ),
                "projected_booked_wear_usd": (
                    projected_net_fee - fill.realized_pnl_delta if fill.fee_known and fill.realized_pnl_known else None
                ),
                "recorded_at_ms": int(time.time() * 1000),
            }
        )
        if not fill.fee_known:
            row["fee_usd"] = None
            row["projected_rebate_total_usd"] = None
        if not fill.realized_pnl_known:
            row["realized_pnl_delta"] = None
        append_jsonl(self.fills_path, row)

    def record_order_events(self, rows: list[dict]) -> None:
        """Append order lifecycle events (place/modify/cancel/reject/flatten).

        Together with the fills and markouts ledgers this makes every order
        the bot ever managed replayable offline: quoting cadence, rejection
        rates, fill ratios and requote behavior can all be reconstructed
        without touching the exchange.
        """
        for row in rows:
            append_jsonl(
                self.orders_path, {"session_id": self.session_id, **row}
            )

    def record_order_intent(self, row: dict) -> None:
        """Persist the cloid/role before a signed, non-idempotent submission."""
        self.orders_path.parent.mkdir(parents=True, exist_ok=True)
        with self.orders_path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps({"session_id": self.session_id, **row}, ensure_ascii=False) + "\n")
            stream.flush()
            os.fsync(stream.fileno())

    def pending_order_intents(self) -> list[tuple[Path, dict]]:
        """Recover both strategies' unresolved sends in this market data root.

        A strategy switch must not erase the previous strategy's UNKNOWN gate.
        Malformed journal lines are evidence gaps, never an empty journal.
        """
        pending: dict[tuple[Path, str], dict] = {}
        pattern = f"{self.mode}_*_{self.coin.replace(':', '_')}_orders.jsonl"
        folder = self.orders_path.parent
        # The GUI isolates grid state under <wallet>/strategies/grid while MM
        # stays at <wallet>. Check exactly that sibling, never another wallet
        # or a recursive archive/backup tree.
        wallet_root = folder.parent.parent if folder.name == "grid" and folder.parent.name == "strategies" else folder
        roots = {folder, wallet_root, wallet_root / "strategies" / "grid"}
        paths = {path for root in roots for path in root.glob(pattern)}
        for path in sorted(paths):
            with path.open(encoding="utf-8") as stream:
                for line_number, line in enumerate(stream, 1):
                    if not line.strip():
                        continue
                    try:
                        row = json.loads(line)
                        if not isinstance(row, dict):
                            raise ValueError("not an object")
                    except (ValueError, TypeError) as exc:
                        raise OrderIntentJournalError(
                            f"订单意图日志 {path.name}:{line_number} 损坏；保持 UNKNOWN，禁止自动重试"
                        ) from exc
                    if row.get("event") not in ORDER_INTENT_EVENTS:
                        continue
                    if not isinstance(row.get("cloid"), str) or not row["cloid"].strip():
                        raise OrderIntentJournalError(
                            f"订单意图日志 {path.name}:{line_number} 缺少有效订单身份；"
                            "保持 UNKNOWN，禁止自动重试"
                        )
                    key = (path, str(row["cloid"]))
                    if order_intent_is_pending(row):
                        pending[key] = row
                    else:
                        pending.pop(key, None)
        return [(path, row) for (path, _cloid), row in pending.items()]

    def record_recovered_order_intent(self, path: Path, row: dict) -> None:
        """Durably close an old intent in its original strategy's journal."""
        with path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps({**row, "recovered_session_id": self.session_id}, ensure_ascii=False) + "\n")
            stream.flush()
            os.fsync(stream.fileno())

    def record_markout(
        self,
        fill: Fill,
        horizon_sec: int,
        mid_px: float,
        *,
        observed_ms: int | None = None,
    ) -> dict:
        direction = 1.0 if fill.is_buy else -1.0
        markout_pnl = (mid_px - fill.price) * fill.qty * direction
        markout_bps = (
            markout_pnl / fill.notional * 10_000 if fill.notional else 0.0
        )
        projected_rebate = (
            fill.projected_self_rebate + fill.projected_referred_benefit
        )
        projected_net_fee = fill.fee_usd - projected_rebate
        observed_ms = observed_ms or int(time.time() * 1000)
        target_ms = fill.time_ms + horizon_sec * 1000
        role_parts = fill.order_role.split(":")
        try:
            level = int(role_parts[-1]) if role_parts and role_parts[-1].isdigit() else None
        except (TypeError, ValueError):
            level = None
        row = {
                "session_id": self.session_id,
                "fill_id": fill.fill_id,
                "order_role": fill.order_role,
                "side": "BUY" if fill.is_buy else "SELL",
                "is_buy": fill.is_buy,
                "level": level,
                "liquidity": fill.liquidity,
                "fill_px": fill.price,
                "fill_qty": fill.qty,
                "horizon_sec": horizon_sec,
                "notional_usd": fill.notional,
                "fill_time_ms": fill.time_ms,
                "target_time_ms": target_ms,
                "observed_time_ms": observed_ms,
                "observation_lag_ms": max(observed_ms - target_ms, 0),
                "mid_px": mid_px,
                "markout_pnl_usd": markout_pnl,
                "markout_bps": markout_bps,
                "fee_known": fill.fee_known,
                "projected_execution_wear_usd": (
                    projected_net_fee - markout_pnl if fill.fee_known else None
                ),
            }
        append_jsonl(self.markouts_path, row)
        return row

    def write_summary(
        self,
        *,
        status: str,
        reason: str | None,
        account: AccountState,
        market: MarketSnapshot,
        fees: FeeModel,
        open_orders: int,
        session_start_equity: float,
        session_started_at_ms: int,
        configured_capital_usd: float,
        session_start_market_unrealized: float = 0.0,
        open_order_details: list[dict] | None = None,
        target_leverage: int | None = None,
        wallet_id: str | None = None,
        risk_profile: dict | None = None,
        adaptive_spread_extra_bps: float = 0.0,
        adaptive_spread_metrics: dict | None = None,
        action_budget: dict | None = None,
        ws_feeds: dict | None = None,
        requote_stats: dict | None = None,
        grid_accounting: dict | None = None,
        recovery: dict | None = None,
    ) -> None:
        fair = market.fair_px
        account_session_net_pnl = account.equity_actual - session_start_equity
        actual_net_pnl = (
            account.realized_pnl
            + account.unrealized_pnl
            - session_start_market_unrealized
            - account.fees_paid
        )
        actual_wear = -actual_net_pnl
        projected_net_pnl = actual_net_pnl + account.projected_rebate_total
        projected_wear = -projected_net_pnl
        accounting_complete = (
            account.fee_accounting_complete and account.realized_accounting_complete
        )
        principal_net_pnl = account.equity_actual - configured_capital_usd
        volume = account.volume_usd
        atomic_write_json(
            self.summary_path,
            {
                "status": status,
                "reason": reason,
                "recovery": recovery,
                "session_id": self.session_id,
                "session_started_at_ms": session_started_at_ms,
                "updated_at_ms": int(time.time() * 1000),
                "configured_capital_usd": configured_capital_usd,
                "wallet_id": wallet_id,
                "risk_profile": risk_profile or {},
                "adaptive_spread_extra_bps": adaptive_spread_extra_bps,
                "adaptive_spread_metrics": adaptive_spread_metrics or {},
                "capital_comparison_assumes_no_external_transfers": True,
                "coin": market.meta.coin,
                "growth_mode": market.meta.growth_mode,
                "isolated_only": market.meta.only_isolated,
                "target_leverage": target_leverage,
                "fair_px": fair,
                "best_bid": market.book.best_bid,
                "best_ask": market.book.best_ask,
                "mark_px": market.mark_px,
                "oracle_px": market.oracle_px,
                "spread_bps": market.book.spread_bps,
                "funding": market.funding,
                "day_notional_volume": market.day_notional_volume,
                "position_qty": account.position_qty,
                "position_notional_usd": account.position_qty * fair if fair else None,
                "entry_px": account.entry_px,
                "realized_pnl": account.realized_pnl if account.realized_accounting_complete else None,
                "fee_accounting_complete": account.fee_accounting_complete,
                "realized_accounting_complete": account.realized_accounting_complete,
                "unrealized_pnl": account.unrealized_pnl,
                "equity_actual": account.equity_actual,
                "account_abstraction": account.account_abstraction,
                "collateral_source": account.collateral_source,
                "available_to_buy_usd": account.available_to_buy_usd,
                "available_to_sell_usd": account.available_to_sell_usd,
                "withdrawable": account.withdrawable,
                "liquidation_px": account.liquidation_px,
                "equity_after_projected_rebate": account.equity_after_projected_rebate,
                "session_start_equity": session_start_equity,
                "remaining_capital_actual": account.equity_actual,
                "principal_net_pnl_usd": principal_net_pnl,
                "principal_wear_usd": -principal_net_pnl,
                "account_session_net_pnl_usd": account_session_net_pnl,
                "account_session_wear_usd": -account_session_net_pnl,
                "actual_net_pnl_usd": actual_net_pnl if accounting_complete else None,
                "actual_wear_usd": actual_wear if accounting_complete else None,
                "actual_wear_bps_of_start_capital": (
                    actual_wear / session_start_equity * 10_000
                    if session_start_equity and accounting_complete
                    else None
                ),
                "actual_wear_bps_per_traded_notional": (
                    actual_wear / volume * 10_000 if volume and accounting_complete else None
                ),
                "projected_net_pnl_after_rebate_usd": projected_net_pnl if accounting_complete else None,
                "projected_wear_after_rebate_usd": projected_wear if accounting_complete else None,
                "projected_wear_bps_per_traded_notional": (
                    projected_wear / volume * 10_000 if volume and accounting_complete else None
                ),
                "turnover_multiple": (
                    volume / session_start_equity if session_start_equity else None
                ),
                "fees_paid": account.fees_paid if account.fee_accounting_complete else None,
                "projected_self_rebate": account.projected_self_rebate,
                "projected_referred_benefit": account.projected_referred_benefit,
                "volume_usd": volume,
                "open_orders": open_orders,
                "open_order_details": open_order_details or [],
                "fill_count": self.fill_count,
                "fee_model": fees.to_dict(),
                "action_budget": action_budget or {},
                "ws_feeds": ws_feeds or {},
                "requote_stats": requote_stats or {},
                "grid_accounting": grid_accounting,
            },
        )
