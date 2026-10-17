"""Offline market-making efficiency analysis.

This module only reads the fill, markout, order-event, and summary ledgers in a
state directory.  It never imports the live broker, reads wallet files, makes a
network request, or mutates a historical ledger.
"""

from __future__ import annotations

import argparse
import bisect
import json
import math
import re
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


ORDER_EVENT_BASES = (
    "place",
    "modify",
    "cancel",
    "replace",
    "inventory_ioc",
    "flatten",
)
NON_ACTION_ORDER_EVENTS = {"ioc_identity", "order_identity"}
ROLE_LEVEL_RE = re.compile(r"(?:^|:)(\d+)$")


def _finite_number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    value = float(value)
    return value if math.isfinite(value) else None


def _positive_number(value: Any) -> float | None:
    value = _finite_number(value)
    return value if value is not None and value > 0 else None


def _side(row: dict[str, Any], role: str) -> str:
    if isinstance(row.get("is_buy"), bool):
        return "buy" if row["is_buy"] else "sell"
    role_lower = role.lower()
    if ":bid:" in role_lower or role_lower.endswith(":bid"):
        return "buy"
    if ":ask:" in role_lower or role_lower.endswith(":ask"):
        return "sell"
    return "unknown"


def _level(role: str) -> int | None:
    match = ROLE_LEVEL_RE.search(role)
    return int(match.group(1)) if match else None


def _symbol(market: str) -> str:
    return market.rsplit(":", 1)[-1].strip().upper()


def _infer_market(prefix: str) -> str:
    match = re.search(r"(?:^|_)io_([^_]+)$", prefix, flags=re.IGNORECASE)
    if match:
        return f"io:{match.group(1)}"
    return prefix.rsplit("_", 1)[-1]


def _canonical_order_event(event: Any) -> str | None:
    if not isinstance(event, str):
        return None
    normalized = event.strip().lower()
    for base in ORDER_EVENT_BASES:
        if normalized == base or normalized.startswith(f"{base}_"):
            return base
    return None


def _read_jsonl(path: Path, diagnostics: Counter[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                diagnostics["malformed_jsonl_rows"] += 1
                continue
            if not isinstance(row, dict):
                diagnostics["non_object_jsonl_rows"] += 1
                continue
            rows.append(row)
    return rows


def _read_summary(path: Path, diagnostics: Counter[str]) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        diagnostics["unreadable_summary_files"] += 1
        return None
    if not isinstance(value, dict):
        diagnostics["non_object_summary_files"] += 1
        return None
    return value


@dataclass
class Metrics:
    fill_count: int = 0
    fill_notional_usd: float = 0.0
    gross_fee_usd: float = 0.0
    projected_net_fee_usd: float = 0.0
    missing_fee_count: int = 0
    missing_projected_fee_count: int = 0
    order_action_count: int = 0
    order_events: Counter[str] = field(default_factory=Counter)
    markout_30_sample_count: int = 0
    markout_30_notional_usd: float = 0.0
    markout_30_pnl_usd: float = 0.0
    adverse_wear_30s_usd: float = 0.0

    def add_fill(self, row: dict[str, Any]) -> None:
        self.fill_count += 1
        self.fill_notional_usd += _positive_number(row.get("notional_usd")) or 0.0
        fee = _finite_number(row.get("fee_usd")) if row.get("fee_known", True) else None
        self.gross_fee_usd += fee or 0.0
        self.missing_fee_count += fee is None
        projected_net_fee = _finite_number(row.get("projected_net_fee_usd"))
        if projected_net_fee is None and fee is not None and "projected_net_fee_usd" not in row:
            rebate = _finite_number(row.get("projected_rebate_total_usd")) or 0.0
            projected_net_fee = fee - rebate
        if fee is None:
            projected_net_fee = None
        self.missing_projected_fee_count += projected_net_fee is None
        self.projected_net_fee_usd += projected_net_fee or 0.0

    def add_action(self, event: str) -> None:
        self.order_action_count += 1
        self.order_events[event] += 1

    def add_markout(self, notional_usd: float, pnl_usd: float) -> None:
        self.markout_30_sample_count += 1
        self.markout_30_notional_usd += notional_usd
        self.markout_30_pnl_usd += pnl_usd
        # Gross toxicity: favorable fills never cancel adverse fills.
        self.adverse_wear_30s_usd += max(0.0, -pnl_usd)

    def merge(self, other: "Metrics") -> None:
        for name in (
            "fill_count",
            "fill_notional_usd",
            "gross_fee_usd",
            "projected_net_fee_usd",
            "missing_fee_count",
            "missing_projected_fee_count",
            "order_action_count",
            "markout_30_sample_count",
            "markout_30_notional_usd",
            "markout_30_pnl_usd",
            "adverse_wear_30s_usd",
        ):
            setattr(self, name, getattr(self, name) + getattr(other, name))
        self.order_events.update(other.order_events)

    def as_dict(self) -> dict[str, Any]:
        action_count = self.order_action_count
        marked_notional = self.markout_30_notional_usd
        return {
            "fill_count": self.fill_count,
            "fill_notional_usd": self.fill_notional_usd,
            "gross_fee_usd": self.gross_fee_usd if not self.missing_fee_count else None,
            "projected_net_fee_usd": self.projected_net_fee_usd if not self.missing_projected_fee_count else None,
            "known_fee_subtotal_usd": self.gross_fee_usd,
            "missing_fee_count": self.missing_fee_count,
            "missing_projected_fee_count": self.missing_projected_fee_count,
            "order_action_count": action_count,
            "order_events": dict(sorted(self.order_events.items())),
            "fills_per_1000_actions": (
                self.fill_count * 1000.0 / action_count if action_count else None
            ),
            "volume_usd_per_action": (
                self.fill_notional_usd / action_count if action_count else None
            ),
            "markout_30_sample_count": self.markout_30_sample_count,
            "markout_30_coverage_pct": (
                self.markout_30_sample_count * 100.0 / self.fill_count
                if self.fill_count
                else None
            ),
            "markout_30_notional_usd": marked_notional,
            "weighted_markout_30s_bps": (
                self.markout_30_pnl_usd / marked_notional * 10_000.0
                if marked_notional
                else None
            ),
            "markout_30_pnl_usd": self.markout_30_pnl_usd,
            "adverse_wear_30s_usd": self.adverse_wear_30s_usd,
            "adverse_wear_30s_bps": (
                self.adverse_wear_30s_usd / marked_notional * 10_000.0
                if marked_notional
                else None
            ),
        }


@dataclass
class SessionData:
    session_id: str
    market: str
    wallet_scope: str = "."
    metrics: Metrics = field(default_factory=Metrics)
    groups: dict[tuple[str, str], Metrics] = field(default_factory=dict)
    fills: list[dict[str, Any]] = field(default_factory=list)
    sources: set[str] = field(default_factory=set)
    modes: set[str] = field(default_factory=set)
    summaries: list[dict[str, Any]] = field(default_factory=list)

    def group(self, role: str, side: str) -> Metrics:
        return self.groups.setdefault((role, side), Metrics())


def _discover_datasets(state_dir: Path) -> dict[str, dict[str, Path]]:
    datasets: dict[str, dict[str, Path]] = {}
    suffixes = {
        "_fills.jsonl": "fills",
        "_markouts.jsonl": "markouts",
        "_orders.jsonl": "orders",
        "_summary.json": "summary",
    }
    paths = list(state_dir.iterdir()) if state_dir.is_dir() else []
    wallets_dir = state_dir / "wallets"
    if wallets_dir.is_dir():
        # Only the canonical named-wallet layout; do not recurse into backups.
        paths.extend(p for wallet in wallets_dir.iterdir() if wallet.is_dir() for p in wallet.iterdir())
    for path in sorted(paths):
        if not path.is_file():
            continue
        for suffix, kind in suffixes.items():
            if path.name.endswith(suffix):
                prefix = path.relative_to(state_dir).as_posix()[: -len(suffix)]
                datasets.setdefault(prefix, {})[kind] = path
                break
    return datasets


def _parse_filter(values: Iterable[str] | None) -> set[str] | None:
    if not values:
        return None
    parsed = {
        _symbol(part)
        for value in values
        for part in value.split(",")
        if part.strip()
    }
    return parsed or None


def _summary_snapshot(path: Path, row: dict[str, Any]) -> dict[str, Any]:
    requote = row.get("requote_stats") if isinstance(row.get("requote_stats"), dict) else {}
    budget = row.get("action_budget") if isinstance(row.get("action_budget"), dict) else {}
    return {
        "source": path.name,
        "status": row.get("status"),
        "updated_at_ms": row.get("updated_at_ms"),
        "session_started_at_ms": row.get("session_started_at_ms"),
        "volume_usd": _finite_number(row.get("volume_usd")),
        "fill_count": row.get("fill_count") if isinstance(row.get("fill_count"), int) else None,
        "requote_modified": (
            requote.get("modified") if isinstance(requote.get("modified"), int) else None
        ),
        "requote_replaced": (
            requote.get("replaced") if isinstance(requote.get("replaced"), int) else None
        ),
        "action_budget_local_spent": _finite_number(budget.get("local_spent")),
        "action_budget_used_official": _finite_number(budget.get("used_official")),
    }


def _sweep_stats(fills: list[dict[str, Any]]) -> dict[str, Any]:
    """Measure level-1 fills with a same-side level-0 fill nearby.

    Each level-1 fill is counted once per window even if several level-0 fills
    are nearby.  The denominator is all level-1 fills for that side.
    """

    by_side: dict[str, list[dict[str, Any]]] = {"buy": [], "sell": []}
    for fill in fills:
        side = fill.get("_side")
        if side in by_side and fill.get("_level") in (0, 1):
            by_side[side].append(fill)

    rows: list[dict[str, Any]] = []
    total = {
        "eligible_level1_fills": 0,
        "eligible_level1_notional_usd": 0.0,
        "within_1s_count": 0,
        "within_1s_notional_usd": 0.0,
        "within_5s_count": 0,
        "within_5s_notional_usd": 0.0,
    }
    for side in ("buy", "sell"):
        level0_times = sorted(
            int(row["_time_ms"])
            for row in by_side[side]
            if row.get("_level") == 0 and row.get("_time_ms") is not None
        )
        level1 = [
            row
            for row in by_side[side]
            if row.get("_level") == 1 and row.get("_time_ms") is not None
        ]
        result = {
            "side": side,
            "eligible_level1_fills": len(level1),
            "eligible_level1_notional_usd": sum(
                row.get("_notional_usd", 0.0) for row in level1
            ),
        }
        nearest: list[tuple[dict[str, Any], int]] = []
        for row in level1:
            time_ms = int(row["_time_ms"])
            position = bisect.bisect_left(level0_times, time_ms)
            candidates = level0_times[max(0, position - 1) : position + 1]
            if candidates:
                nearest.append((row, min(abs(time_ms - candidate) for candidate in candidates)))
        for seconds in (1, 5):
            matched = [row for row, delta in nearest if delta <= seconds * 1000]
            count = len(matched)
            notional = sum(row.get("_notional_usd", 0.0) for row in matched)
            result[f"within_{seconds}s_count"] = count
            result[f"within_{seconds}s_notional_usd"] = notional
            result[f"within_{seconds}s_count_pct"] = (
                count * 100.0 / len(level1) if level1 else None
            )
            denominator_notional = result["eligible_level1_notional_usd"]
            result[f"within_{seconds}s_notional_pct"] = (
                notional * 100.0 / denominator_notional if denominator_notional else None
            )
        rows.append(result)
        for key in total:
            total[key] += result[key]

    eligible_count = total["eligible_level1_fills"]
    eligible_notional = total["eligible_level1_notional_usd"]
    for seconds in (1, 5):
        total[f"within_{seconds}s_count_pct"] = (
            total[f"within_{seconds}s_count"] * 100.0 / eligible_count
            if eligible_count
            else None
        )
        total[f"within_{seconds}s_notional_pct"] = (
            total[f"within_{seconds}s_notional_usd"] * 100.0 / eligible_notional
            if eligible_notional
            else None
        )
    return {"overall": total, "by_side": rows}


def _add_summary_comparison(snapshot: dict[str, Any], ledger: dict[str, Any]) -> None:
    summary_volume = snapshot.get("volume_usd")
    summary_fills = snapshot.get("fill_count")
    snapshot["ledger_comparison"] = {
        "fill_notional_delta_usd": (
            ledger["fill_notional_usd"] - summary_volume
            if summary_volume is not None
            else None
        ),
        "fill_count_delta": (
            ledger["fill_count"] - summary_fills if summary_fills is not None else None
        ),
        "modify_count_delta": (
            ledger["order_events"].get("modify", 0) - snapshot["requote_modified"]
            if snapshot.get("requote_modified") is not None
            else None
        ),
    }


def analyze(
    state_dir: str | Path,
    *,
    symbols: Iterable[str] | None = None,
    session_ids: Iterable[str] | None = None,
) -> dict[str, Any]:
    """Return a JSON-serializable efficiency report without changing *state_dir*."""

    root = Path(state_dir).expanduser().resolve()
    symbol_filter = _parse_filter(symbols)
    session_filter = set(session_ids) if session_ids else None
    diagnostics: Counter[str] = Counter()
    datasets = _discover_datasets(root)
    sessions: dict[tuple[str, str, str], SessionData] = {}
    input_files: set[str] = set()
    seen_cancel_actions: set[tuple[str, str, str, str]] = set()

    def wanted(session_id: str, market: str) -> bool:
        return (
            (session_filter is None or session_id in session_filter)
            and (symbol_filter is None or _symbol(market) in symbol_filter)
        )

    def get_session(session_id: str, market: str, prefix: str, mode: Any = None) -> SessionData:
        scope = str(Path(prefix).parent).replace("\\", "/")
        key = (scope, session_id, market)
        value = sessions.setdefault(key, SessionData(session_id=session_id, market=market, wallet_scope=scope))
        value.sources.add(prefix)
        if isinstance(mode, str) and mode:
            value.modes.add(mode)
        elif Path(prefix).name.startswith("live_"):
            value.modes.add("live")
        elif Path(prefix).name.startswith("paper_"):
            value.modes.add("paper")
        return value

    for prefix, paths in sorted(datasets.items()):
        for path in paths.values():
            input_files.add(path.relative_to(root).as_posix())
        summary = _read_summary(paths["summary"], diagnostics) if "summary" in paths else None
        default_market = (
            str(summary.get("coin"))
            if summary and isinstance(summary.get("coin"), str) and summary.get("coin")
            else _infer_market(prefix)
        )

        fills_by_key: dict[tuple[str, str], dict[str, Any]] = {}
        fills_by_id: dict[str, list[dict[str, Any]]] = {}
        fills_path = paths.get("fills")
        if fills_path:
            for row_number, row in enumerate(_read_jsonl(fills_path, diagnostics), start=1):
                session_id = str(row.get("session_id") or "<unknown>")
                market = str(row.get("coin") or default_market)
                if not wanted(session_id, market):
                    continue
                role = str(row.get("order_role") or "<unknown>")
                side = _side(row, role)
                notional = _positive_number(row.get("notional_usd")) or 0.0
                time_ms = _finite_number(row.get("time_ms"))
                normalized = dict(row)
                normalized.update(
                    {
                        "_role": role,
                        "_side": side,
                        "_level": _level(role),
                        "_notional_usd": notional,
                        "_time_ms": int(time_ms) if time_ms is not None else None,
                        "_session_id": session_id,
                        "_market": market,
                    }
                )
                fill_id = str(row.get("fill_id") or f"<row:{row_number}>")
                fill_key = (session_id, fill_id)
                if fill_key in fills_by_key:
                    diagnostics["duplicate_fill_rows"] += 1
                    continue
                fills_by_key[fill_key] = normalized
                fills_by_id.setdefault(fill_id, []).append(normalized)
                session = get_session(session_id, market, prefix, row.get("mode"))
                session.fills.append(normalized)
                session.metrics.add_fill(row)
                session.group(role, side).add_fill(row)

        orders_path = paths.get("orders")
        if orders_path:
            for row in _read_jsonl(orders_path, diagnostics):
                session_id = str(row.get("session_id") or "<unknown>")
                market = str(row.get("coin") or default_market)
                if not wanted(session_id, market):
                    continue
                raw_event = str(row.get("event") or "").strip().lower()
                if raw_event in NON_ACTION_ORDER_EVENTS:
                    continue
                canonical_event = _canonical_order_event(raw_event)
                if canonical_event is None:
                    diagnostics["unknown_order_event_rows"] += 1
                    continue
                if canonical_event == "cancel":
                    cloid = str(row.get("cloid") or "").lower()
                    if cloid:
                        attempt = row.get("attempt")
                        action_identity = (
                            f"attempt:{attempt}"
                            if isinstance(attempt, int)
                            and not isinstance(attempt, bool)
                            and attempt > 0
                            else "legacy"
                        )
                        cancel_key = (
                            str(Path(prefix).parent),
                            session_id,
                            market,
                            cloid,
                            action_identity,
                        )
                        if cancel_key in seen_cancel_actions:
                            # Older ledgers wrote another cancel row on every
                            # reconciliation poll without an attempt number.
                            # New bounded retries are distinct real actions,
                            # but duplicate persistence of one attempt is not.
                            diagnostics["duplicate_cancel_action_rows"] += 1
                            continue
                        seen_cancel_actions.add(cancel_key)
                event = raw_event
                role = str(row.get("role") or row.get("order_role") or "<unknown>")
                side = _side(row, role)
                session = get_session(session_id, market, prefix, row.get("mode"))
                session.metrics.add_action(event)
                session.group(role, side).add_action(event)

        markouts_path = paths.get("markouts")
        if markouts_path:
            markouts_30: dict[tuple[str, str], tuple[dict[str, Any], dict[str, Any] | None]] = {}
            for row in _read_jsonl(markouts_path, diagnostics):
                if _finite_number(row.get("horizon_sec")) != 30.0:
                    continue
                fill_id = str(row.get("fill_id") or "")
                row_session_id = str(row.get("session_id") or "")
                candidates = fills_by_id.get(fill_id, [])
                fill = next(
                    (
                        candidate
                        for candidate in candidates
                        if row_session_id and candidate.get("_session_id") == row_session_id
                    ),
                    None,
                )
                if fill is None and len(candidates) == 1:
                    fill = candidates[0]
                elif fill is None and len(candidates) > 1:
                    diagnostics["ambiguous_markout_fill_joins"] += 1
                session_id = str(
                    (fill or {}).get("_session_id") or row.get("session_id") or "<unknown>"
                )
                market = str((fill or {}).get("_market") or row.get("coin") or default_market)
                if not wanted(session_id, market):
                    continue
                if fill is None:
                    diagnostics["unmatched_markout_30_rows"] += 1
                elif row.get("session_id") not in (None, session_id):
                    diagnostics["markout_session_mismatches"] += 1
                key = (session_id, fill_id)
                prior = markouts_30.get(key)
                if prior is not None:
                    diagnostics["duplicate_markout_30_rows"] += 1
                    old_observed = _finite_number(prior[0].get("observed_time_ms")) or -1.0
                    new_observed = _finite_number(row.get("observed_time_ms")) or -1.0
                    if new_observed < old_observed:
                        continue
                markouts_30[key] = (row, fill)

            for (session_id, _), (row, fill) in markouts_30.items():
                market = str((fill or {}).get("_market") or row.get("coin") or default_market)
                role = str((fill or {}).get("_role") or row.get("order_role") or "<unknown>")
                side = str((fill or {}).get("_side") or _side(row, role))
                notional = _positive_number(row.get("notional_usd"))
                if notional is None and fill is not None:
                    notional = _positive_number(fill.get("_notional_usd"))
                pnl = _finite_number(row.get("markout_pnl_usd"))
                if notional is None or pnl is None:
                    diagnostics["invalid_markout_30_rows"] += 1
                    continue
                session = get_session(session_id, market, prefix, (fill or {}).get("mode"))
                session.metrics.add_markout(notional, pnl)
                session.group(role, side).add_markout(notional, pnl)

        if summary:
            session_id = str(summary.get("session_id") or "")
            market = str(summary.get("coin") or default_market)
            if session_id and wanted(session_id, market):
                session = get_session(session_id, market, prefix, summary.get("mode"))
                session.summaries.append(_summary_snapshot(paths["summary"], summary))

    total_metrics = Metrics()
    session_rows: list[dict[str, Any]] = []
    sweep_totals = {
        "eligible_level1_fills": 0,
        "eligible_level1_notional_usd": 0.0,
        "within_1s_count": 0,
        "within_1s_notional_usd": 0.0,
        "within_5s_count": 0,
        "within_5s_notional_usd": 0.0,
    }
    for session in sorted(sessions.values(), key=lambda row: (row.market, row.session_id)):
        total_metrics.merge(session.metrics)
        metrics = session.metrics.as_dict()
        sweep = _sweep_stats(session.fills)
        for key in sweep_totals:
            sweep_totals[key] += sweep["overall"][key]
        for snapshot in session.summaries:
            _add_summary_comparison(snapshot, metrics)
        groups = []
        for (role, side), group_metrics in sorted(session.groups.items()):
            groups.append(
                {
                    "role": role,
                    "level": _level(role),
                    "side": side,
                    "metrics": group_metrics.as_dict(),
                }
            )
        session_rows.append(
            {
                "session_id": session.session_id,
                "wallet_scope": session.wallet_scope,
                "market": session.market,
                "symbol": _symbol(session.market),
                "sources": sorted(session.sources),
                "modes": sorted(session.modes),
                "metrics": metrics,
                "same_side_level_sweeps": sweep,
                "summary_snapshots": session.summaries,
                "groups": groups,
            }
        )

    eligible_count = sweep_totals["eligible_level1_fills"]
    eligible_notional = sweep_totals["eligible_level1_notional_usd"]
    for seconds in (1, 5):
        sweep_totals[f"within_{seconds}s_count_pct"] = (
            sweep_totals[f"within_{seconds}s_count"] * 100.0 / eligible_count
            if eligible_count
            else None
        )
        sweep_totals[f"within_{seconds}s_notional_pct"] = (
            sweep_totals[f"within_{seconds}s_notional_usd"] * 100.0 / eligible_notional
            if eligible_notional
            else None
        )

    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "inputs": {
            "state_dir": str(root),
            "symbols": sorted(symbol_filter) if symbol_filter else None,
            "session_ids": sorted(session_filter) if session_filter else None,
            "files_read": sorted(input_files),
            "diagnostics": dict(sorted(diagnostics.items())),
        },
        "definitions": {
            "order_action": (
                "orders.jsonl 中每条 place/modify/cancel/replace/inventory_ioc/"
                "flatten 及其 *_rejected "
                "记录计 1 个订单子动作；未知事件不计入。"
            ),
            "weighted_markout_30s_bps": (
                "30 秒后方向化 markout PnL 之和 / 已观测名义额之和 * 10000；"
                "正值对做市者有利，负值表示逆向选择。"
            ),
            "adverse_wear_30s": (
                "sum(max(0, -markout_pnl_usd))；只累加 30 秒不利 markout，"
                "不用有利 markout 抵消，不包手续费或返佣。"
            ),
            "same_side_level_sweep": (
                "以每个 level1 成交为分母；同 session/市场/方向的 level0 "
                "成交时间差绝对值 <= 1秒或 5秒时，level1 计为同扫一次。"
            ),
            "summary_usage": (
                "summary 仅作当前会话快照对账，不替代 JSONL 历史口径。"
            ),
        },
        "totals": {
            "metrics": total_metrics.as_dict(),
            "same_side_level_sweeps": sweep_totals,
        },
        "sessions": session_rows,
    }


def _fmt(value: Any, decimals: int = 2, suffix: str = "") -> str:
    if value is None:
        return "-"
    if isinstance(value, int):
        return f"{value:,}{suffix}"
    return f"{float(value):,.{decimals}f}{suffix}"


def render_table(report: dict[str, Any]) -> str:
    """Render a compact Chinese table for interactive inspection."""

    side_labels = {"buy": "买", "sell": "卖", "unknown": "未知"}
    total = report["totals"]["metrics"]
    lines = [
        "历史做市效率（30秒 markout）",
        (
            f"合计：成交 {total['fill_count']:,} 笔 / "
            f"${total['fill_notional_usd']:,.2f}，订单动作 {total['order_action_count']:,}，"
            f"$/动作 {_fmt(total['volume_usd_per_action'])}，"
            f"加权 markout {_fmt(total['weighted_markout_30s_bps'], suffix=' bps')}，"
            f"不利磨损 {_fmt(total['adverse_wear_30s_bps'], suffix=' bps')}"
        ),
        "不利磨损 = 逐笔 sum(max(0, -30秒 markout PnL))，不与有利 markout 互抵，不含手续费/返佣。",
        "",
        "会话      市场       角色          方向  成交笔  成交额($)  动作数  fills/千动作  $/动作  30s markout  不利磨损",
        "-" * 116,
    ]
    for session in report["sessions"]:
        short_session = session["session_id"][:8]
        for group in session["groups"]:
            metrics = group["metrics"]
            lines.append(
                f"{short_session:<9} {session['market']:<10} {group['role']:<13} "
                f"{side_labels.get(group['side'], group['side']):<5} {metrics['fill_count']:>6} "
                f"{metrics['fill_notional_usd']:>10.2f} {metrics['order_action_count']:>7} "
                f"{_fmt(metrics['fills_per_1000_actions']):>13} "
                f"{_fmt(metrics['volume_usd_per_action']):>7} "
                f"{_fmt(metrics['weighted_markout_30s_bps'], suffix='bp'):>12} "
                f"{_fmt(metrics['adverse_wear_30s_bps'], suffix='bp'):>10}"
            )

    lines.extend(
        [
            "",
            "同方向多层同扫（以 level1 成交为分母）",
            "会话      市场       方向  level1笔数  1秒同扫  5秒同扫",
            "-" * 67,
        ]
    )
    for session in report["sessions"]:
        for row in session["same_side_level_sweeps"]["by_side"]:
            if not row["eligible_level1_fills"]:
                continue
            lines.append(
                f"{session['session_id'][:8]:<9} {session['market']:<10} "
                f"{side_labels.get(row['side'], row['side']):<5} "
                f"{row['eligible_level1_fills']:>10} "
                f"{_fmt(row['within_1s_count_pct'], suffix='%'):>9} "
                f"{_fmt(row['within_5s_count_pct'], suffix='%'):>9}"
            )
    return "\n".join(lines)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="纯离线分析做市成交、订单动作、30秒 markout 和多层同扫。"
    )
    parser.add_argument("--state-dir", default="state", help="历史台账目录（默认 state）")
    parser.add_argument(
        "--symbols",
        nargs="+",
        help="一个或多个市场代号，支持 'SNDK ANTH' 或 'SNDK,ANTH'",
    )
    parser.add_argument(
        "--session-id",
        action="append",
        dest="session_ids",
        help="只分析指定 session；可重复传入",
    )
    parser.add_argument("--format", choices=("table", "json", "both"), default="both")
    parser.add_argument("--json-out", help="可选 JSON 输出路径；不传则不写文件")
    return parser


def _ensure_utf8_console() -> None:
    if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
        try:
            sys.stdout.reconfigure(encoding="utf-8")
            sys.stderr.reconfigure(encoding="utf-8")
        except (AttributeError, OSError):
            pass


def main(argv: list[str] | None = None) -> int:
    _ensure_utf8_console()
    args = _parser().parse_args(argv)
    report = analyze(
        args.state_dir,
        symbols=args.symbols,
        session_ids=args.session_ids,
    )
    json_text = json.dumps(report, ensure_ascii=False, indent=2)
    if args.json_out:
        Path(args.json_out).expanduser().write_text(json_text + "\n", encoding="utf-8")
    if args.format in ("table", "both"):
        print(render_table(report))
    if args.format == "both":
        print("\n--- JSON ---")
    if args.format in ("json", "both"):
        print(json_text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
