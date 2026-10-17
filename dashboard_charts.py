"""Chart-ready local fills; never fabricate equity or missing fee/PnL values."""
from __future__ import annotations
import math


def finite(value):
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) else None


def build_charts(fills, *, profile_id, session_id, wallet_id, coin, mode):
    rows, seen, missing = [], set(), 0
    for index, row in enumerate(fills):
        identity = row.get("fill_id") or f"row:{index}"
        if identity in seen:
            continue
        seen.add(identity)
        time = finite(row.get("time_ms"))
        if time is None or time <= 0:
            missing += 1
            continue
        rows.append(row)
    rows.sort(key=lambda r: r["time_ms"])
    start, end = (int(rows[0]["time_ms"]), int(rows[-1]["time_ms"])) if rows else (None, None)
    bucket_ms = max(60000, math.ceil((end - start) / 240 / 60000) * 60000) if rows else 60000
    volume, fee, pnl = 0.0, 0.0, 0.0
    valid_volume = valid_fee = valid_pnl = True
    sides = {side: {"count": 0, "volume_usd": 0.0} for side in ("buy", "sell", "unknown")}
    buckets = {}
    for row in rows:
        notional = finite(row.get("notional_usd"))
        paid = finite(row.get("fee_usd")) if row.get("fee_known", True) else None
        realized = finite(row.get("realized_pnl_delta")) if row.get("realized_pnl_known", True) else None
        valid_volume &= notional is not None and notional > 0
        valid_fee &= paid is not None
        valid_pnl &= realized is not None
        volume += notional or 0.0
        fee += paid or 0.0
        pnl += realized or 0.0
        side = "buy" if row.get("is_buy") is True else "sell" if row.get("is_buy") is False else "unknown"
        sides[side]["count"] += 1
        if notional is None or notional <= 0:
            sides[side]["volume_usd"] = None
        elif sides[side]["volume_usd"] is not None:
            sides[side]["volume_usd"] += notional
        bucket = int(row["time_ms"]) // bucket_ms
        buckets[bucket] = {"time_ms": int(row["time_ms"]),
                           "cumulative_volume_usd": volume if valid_volume else None,
                           "cumulative_fee_usd": fee if valid_fee else None,
                           "cumulative_realized_pnl_usd": pnl if valid_pnl else None,
                           "cumulative_realized_less_fees_usd": pnl - fee if valid_pnl and valid_fee else None}
    return {"scope": {"profile_id": profile_id, "session_id": session_id, "wallet_id": wallet_id,
                       "coin": coin, "mode": mode, "from_ms": start, "to_ms": end,
                       "fill_count": len(rows), "complete": bool(rows) and valid_volume and valid_fee and valid_pnl and not missing,
                       "missing_time_rows": missing, "bucket_seconds": bucket_ms // 1000},
            "series": list(buckets.values()), "sides": sides,
            "note": "本地已保存成交按时间分桶取累计值；已实现价差盈亏减手续费，未计资金费、浮盈亏及返佣。"}
