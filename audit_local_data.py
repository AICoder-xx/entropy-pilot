"""Read-only local Entropy inventory and aggregate report; no wallet access/network."""
from __future__ import annotations
import argparse
import hashlib
import json
import math
from collections import Counter
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from analyze_efficiency import analyze, _discover_datasets


def number(v):
    return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v) else None


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def timestamp(value):
    return datetime.fromtimestamp(value / 1000, ZoneInfo("Asia/Taipei")).isoformat() if value else None


def audit(root: Path, legacy: Path | None = None):
    state = root / "state"
    files, duplicate_files = [], []
    for path in sorted(state.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(state)
        record = {"path": relative.as_posix(), "bytes": path.stat().st_size, "sha256": digest(path)}
        files.append(record)
        other = legacy / relative if legacy else None
        if other and other.is_file() and digest(other) == record["sha256"]:
            duplicate_files.append(relative.as_posix())
    rows, diagnostics, seen = [], Counter(), {}
    paper_count = 0
    fill_paths = [d["fills"] for d in _discover_datasets(state).values() if "fills" in d]
    identity_rows = {}
    for path in sorted(fill_paths):
        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
                if not isinstance(row, dict):
                    raise ValueError()
            except ValueError:
                diagnostics["malformed_fill_rows"] += 1
                continue
            if row.get("mode") != "live":
                paper_count += 1
                continue
            # Cross-source copies from the same exchange execution are excluded.
            # Conflicting identities are disclosed, never silently summed.
            identity = (row.get("coin"), row.get("fill_id"), row.get("oid"), row.get("time_ms"))
            if any(value is None for value in identity):
                diagnostics["missing_fill_identity_rows"] += 1
                row["_conflict"] = True
                identity = ("missing", str(path), line_number)
            signature = tuple(row.get(k) for k in ("price", "qty", "is_buy", "fee_usd", "realized_pnl_delta"))
            if identity in seen:
                diagnostics["duplicate_fill_rows" if seen[identity] == signature else "conflicting_fill_identities"] += 1
                if seen[identity] != signature:
                    identity_rows[identity]["_conflict"] = True
                continue
            seen[identity] = signature
            identity_rows[identity] = row
            row["source"] = str(path.parent.relative_to(state)).replace("\\", "/")
            rows.append(row)
    efficiency = analyze(state)
    def summarize(group):
        count = len(group)
        volume = sum(number(r.get("notional_usd")) or 0 for r in group)
        missing_notional = sum(number(r.get("notional_usd")) is None or (number(r.get("notional_usd")) or 0) <= 0 for r in group)
        conflicted = any(r.get("_conflict") for r in group)
        fees = [number(r.get("fee_usd")) if r.get("fee_known", True) else None for r in group]
        pnl = [number(r.get("realized_pnl_delta")) if r.get("realized_pnl_known", True) else None for r in group]
        known = not conflicted and all(x is not None for x in fees + pnl)
        realized = sum(x for x in pnl if x is not None)
        fee = sum(x for x in fees if x is not None)
        times = [r["time_ms"] for r in group if number(r.get("time_ms"))]
        return {"fills": count, "complete": known and not missing_notional,
                "missing_notional_rows": missing_notional, "conflicting_or_missing_identity": conflicted,
                "volume_usd": volume if not conflicted and not missing_notional else None,
                "fee_usd": fee if not conflicted and all(x is not None for x in fees) else None,
                "realized_price_pnl_usd": realized if not conflicted and all(x is not None for x in pnl) else None,
                "realized_less_fees_usd": realized - fee if known else None,
                "booked_cost_per_million_usd": (fee - realized) / volume * 1e6 if known and volume and not missing_notional else None,
                "missing_fee_rows": fees.count(None), "missing_pnl_rows": pnl.count(None),
                "liquidity": dict(Counter(r.get("liquidity", "unknown") for r in group)),
                "first_fill": timestamp(min(times)) if times else None,
                "last_fill": timestamp(max(times)) if times else None}
    markets = {coin: summarize([r for r in rows if r.get("coin") == coin]) for coin in sorted({r.get("coin") for r in rows})}
    sources = {source: summarize([r for r in rows if r["source"] == source]) for source in sorted({r["source"] for r in rows})}
    for coin, stats in markets.items():
        sessions = [s for s in efficiency["sessions"] if s["market"] == coin and s["modes"] == ["live"]]
        metrics = [s["metrics"] for s in sessions]
        action_count = sum(m["order_action_count"] for m in metrics)
        mark_count = sum(m["markout_30_sample_count"] for m in metrics)
        mark_volume = sum(m["markout_30_notional_usd"] for m in metrics)
        mark_pnl = sum(m["markout_30_pnl_usd"] for m in metrics)
        stats.update(order_actions_recorded=action_count, volume_per_recorded_action=stats["volume_usd"] / action_count if action_count and stats["volume_usd"] is not None else None,
                     markout_30_samples=mark_count, markout_30_coverage_pct=mark_count / stats["fills"] * 100 if stats["fills"] else None,
                     weighted_markout_30_bps=mark_pnl / mark_volume * 10000 if mark_volume else None)
    return {"generated_at": datetime.now(ZoneInfo("Asia/Taipei")).isoformat(), "total": summarize(rows), "markets": markets,
            "sources": sources, "paper_fills_excluded": paper_count, "diagnostics": dict(diagnostics),
            "input_files": files, "historical_copy_identical_files_excluded": duplicate_files,
            "efficiency": efficiency,
            "limitations": ["只统计本地已保存成交，不能代表交易所完整历史。", "资金费、跨期未实现盈亏、外部转账及已到账返佣不在逐笔成交口径内；全账户最终净收益未知。",
                             "旧日志未完整记录所有下单动作，美元/动作只能按已记录动作计算。", "30秒 markout 是成交质量指标，不是最终亏损。",
                             "返佣字段是历史投影，不作为已到账收益；不同策略/仓位/窗口不可直接作为收益 A/B。"]}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".")
    parser.add_argument("--legacy-state")
    parser.add_argument("--output", default="reports/local-data.json")
    args = parser.parse_args()
    report = audit(Path(args.root).resolve(), Path(args.legacy_state).resolve() if args.legacy_state else None)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({k: report[k] for k in ["total", "markets", "sources", "paper_fills_excluded", "diagnostics"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
