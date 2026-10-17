"""Read-only ledger views, telemetry conversion and incremental caches.

The repository receives a data root and reader; it has no controller,
credential loader, signer or network client. Cache ownership is per instance.
"""
from __future__ import annotations

import heapq
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable


def _number(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def calculate_exposure(
    position_qty: float,
    mark_px: float,
    orders: list[dict],
) -> dict:
    buy_qty = sum(
        _number(item.get("remaining_qty"))
        for item in orders
        if item.get("is_buy")
    )
    sell_qty = sum(
        _number(item.get("remaining_qty"))
        for item in orders
        if not item.get("is_buy")
    )
    return {
        "current_long_usd": max(position_qty, 0.0) * mark_px,
        "current_short_usd": max(-position_qty, 0.0) * mark_px,
        "pending_buy_usd": sum(
            _number(item.get("notional_usd"))
            for item in orders
            if item.get("is_buy")
        ),
        "pending_sell_usd": sum(
            _number(item.get("notional_usd"))
            for item in orders
            if not item.get("is_buy")
        ),
        "worst_case_long_usd": max(position_qty + buy_qty, 0.0) * mark_px,
        "worst_case_short_usd": max(-(position_qty - sell_qty), 0.0) * mark_px,
        "net_position_usd": position_qty * mark_px,
    }


def overlay_fresh_bot_telemetry(live: dict, summary: dict) -> dict:
    """Overlay cheap, reconciled bot telemetry on a slow official snapshot.

    The official REST snapshot remains available as the low-rate independent
    check.  While a current bot summary is fresh, the UI should not pretend its
    orders/position are minutes old merely because we deliberately gave the
    trading processes priority in the shared IP quota.
    """
    if not live.get("ok") or summary.get("status") not in {"running", "paused"}:
        return live
    updated_at_ms = int(summary.get("updated_at_ms") or 0)
    if updated_at_ms <= 0 or int(time.time() * 1000) - updated_at_ms > 15_000:
        return live
    orders = summary.get("open_order_details")
    if not isinstance(orders, list):
        return live

    value = dict(live)
    value["local_telemetry_updated_at_ms"] = updated_at_ms
    value["telemetry_source"] = "bot_reconciled"
    value["open_orders"] = list(orders)

    for target, source in (
        ("monitored_equity_usd", "equity_actual"),
        ("available_to_buy_usd", "available_to_buy_usd"),
        ("available_to_sell_usd", "available_to_sell_usd"),
        ("free_collateral_usd", "withdrawable"),
        ("io_withdrawable", "withdrawable"),
    ):
        if summary.get(source) is not None:
            value[target] = summary[source]

    position = dict(value.get("position") or {})
    for target, source in (
        ("qty", "position_qty"),
        ("entry_px", "entry_px"),
        ("unrealized_pnl_usd", "unrealized_pnl"),
        ("liquidation_px", "liquidation_px"),
    ):
        if summary.get(source) is not None:
            position[target] = summary[source]
    value["position"] = position

    market = dict(value.get("market") or {})
    for target, source in (
        ("best_bid", "best_bid"),
        ("best_ask", "best_ask"),
        ("spread_bps", "spread_bps"),
        ("mark_px", "mark_px"),
        ("oracle_px", "oracle_px"),
        ("funding", "funding"),
        ("day_notional_volume", "day_notional_volume"),
    ):
        if summary.get(source) is not None:
            market[target] = summary[source]
    value["market"] = market
    mark_px = _number(summary.get("mark_px") or summary.get("fair_px"))
    value["exposure"] = calculate_exposure(
        _number(summary.get("position_qty")), mark_px, orders
    )
    return value


def live_from_bot_summary(summary: dict) -> dict:
    """Build the running dashboard view without consuming official REST."""
    orders = summary.get("open_order_details")
    if not isinstance(orders, list):
        orders = []
    mark_px = _number(summary.get("mark_px") or summary.get("fair_px"))
    leverage = summary.get("target_leverage")
    updated_at_ms = int(summary.get("updated_at_ms") or 0)
    telemetry_age_ms = (
        max(int(time.time() * 1000) - updated_at_ms, 0)
        if updated_at_ms
        else None
    )
    return {
        "ok": True,
        "updated_at_ms": 0,
        "local_telemetry_updated_at_ms": updated_at_ms,
        "official_snapshot_paused": True,
        "telemetry_stale": (
            telemetry_age_ms is None
            or telemetry_age_ms > 15_000
            or summary.get("status") not in {"running", "paused"}
        ),
        "telemetry_age_ms": telemetry_age_ms,
        "telemetry_source": "bot_reconciled",
        "bot_status": summary.get("status"),
        "bot_status_reason": summary.get("reason"),
        "coin": summary.get("coin"),
        "role": "bot-verified",
        "account_abstraction": summary.get("account_abstraction"),
        "collateral_source": summary.get("collateral_source"),
        "monitored_equity_usd": summary.get("equity_actual"),
        "spot_usdc_total": None,
        "io_account_value": None,
        "io_withdrawable": summary.get("withdrawable"),
        "available_to_buy_usd": summary.get("available_to_buy_usd"),
        "available_to_sell_usd": summary.get("available_to_sell_usd"),
        "free_collateral_usd": summary.get("withdrawable"),
        "position": {
            "qty": summary.get("position_qty"),
            "entry_px": summary.get("entry_px"),
            "unrealized_pnl_usd": summary.get("unrealized_pnl"),
            "liquidation_px": summary.get("liquidation_px"),
            "margin_used_usd": None,
            "leverage": {"type": "isolated", "value": leverage}
            if leverage
            else None,
        },
        "market": {
            "best_bid": summary.get("best_bid"),
            "best_ask": summary.get("best_ask"),
            "spread_bps": summary.get("spread_bps"),
            "mark_px": summary.get("mark_px"),
            "oracle_px": summary.get("oracle_px"),
            "funding": summary.get("funding"),
            "day_notional_volume": summary.get("day_notional_volume"),
            "growth_mode": bool(summary.get("growth_mode")),
            "isolated_only": bool(summary.get("isolated_only")),
        },
        "exposure": calculate_exposure(
            _number(summary.get("position_qty")), mark_px, orders
        ),
        "open_orders": list(orders),
        "recent_fills": [],
        "fills_updated_at_ms": 0,
        "fee_model": summary.get("fee_model"),
    }


@dataclass(frozen=True)
class SnapshotServices:
    STATE_DIR: Path
    read_json: Callable
    build_charts: Callable


class SnapshotRepository:
    def __init__(self, services: Callable[[], SnapshotServices]) -> None:
        self._services = services
        self.jsonl_cache: dict[Path, dict] = {}
        self.session_cache: dict[tuple, dict] = {}

    def read_jsonl(self, path: Path) -> list[dict]:
        return list(self._jsonl_data(path)["rows"])


    def _jsonl_data(self, path: Path) -> dict:
        """Incrementally read complete lines and retain cheap lifetime totals."""
        cached = self.jsonl_cache.setdefault(path, {"offset": 0, "rows": [], "volume": 0.0, "fees": 0.0})
        try:
            stat = path.stat()
            size = stat.st_size
            identity = (stat.st_dev, stat.st_ino)
            if (size < cached["offset"] or cached.get("identity", identity) != identity
                    or (size == cached.get("size") and stat.st_mtime_ns != cached.get("mtime_ns"))):
                cached = {"offset": 0, "rows": [], "volume": 0.0, "fees": 0.0}
                self.jsonl_cache[path] = cached
            cached.update(identity=identity, size=size, mtime_ns=stat.st_mtime_ns)
            if size == cached["offset"]:
                return cached
            with path.open("rb") as handle:
                handle.seek(cached["offset"])
                for raw in handle:
                    row_start = handle.tell() - len(raw)
                    if not raw.endswith(b"\n"):
                        handle.seek(row_start)
                        break
                    try:
                        value = json.loads(raw.decode("utf-8"))
                    except (UnicodeDecodeError, json.JSONDecodeError):
                        continue
                    if isinstance(value, dict):
                        cached["rows"].append(value)
                        cached["volume"] += _number(value.get("notional_usd"))
                        cached["fees"] += _number(value.get("fee_usd"))
                cached["offset"] = handle.tell()
        except OSError:
            pass
        return cached


    def profiles(self) -> list[dict]:
        deps = self._services()
        result = []
        for path in deps.STATE_DIR.rglob("*_summary.json"):
            summary = deps.read_json(path, {})
            if not isinstance(summary, dict):
                continue
            relative = path.relative_to(deps.STATE_DIR).as_posix()
            profile_id = relative.removesuffix("_summary.json")
            mode = str(summary.get("mode") or "")
            strategy = str(summary.get("strategy") or "")
            if not mode:
                mode = "live" if path.name.startswith("live_") else "paper"
            if not strategy:
                strategy = "grid" if "_grid_" in path.name else "market_maker"
            result.append(
                {
                    "id": profile_id,
                    "coin": summary.get("coin"),
                    "status": summary.get("status"),
                    "mode": mode,
                    "strategy": strategy,
                    "wallet_id": summary.get("wallet_id") or "primary",
                    "updated_at_ms": summary.get("updated_at_ms")
                    or int(path.stat().st_mtime * 1000),
                }
            )
        # Once a named-wallet runtime exists, the root-level ``primary`` live
        # summaries are stale compatibility artifacts from before import.  Do not
        # keep serving them to an already-open browser: an old selected profile
        # must automatically fall through to the current named-wallet session.
        has_named_live_profile = any(
            item["mode"] == "live" and item["id"].startswith("wallets/")
            for item in result
        )
        if has_named_live_profile:
            result = [
                item
                for item in result
                if not (
                    item["mode"] == "live"
                    and item["wallet_id"] == "primary"
                    and not item["id"].startswith("wallets/")
                )
            ]
        result.sort(key=lambda row: row["updated_at_ms"] or 0, reverse=True)
        return result


    def _session_ledger_view(self, fills_path: Path, markouts_path: Path, session_id: str | None) -> dict:
        fill_data, markout_data = self._jsonl_data(fills_path), self._jsonl_data(markouts_path)
        key = (fills_path, markouts_path, session_id)
        stamp = tuple((id(data), data.get("identity"), data.get("mtime_ns"), data["offset"]) for data in (fill_data, markout_data))
        cached = self.session_cache.get(key)
        if cached and cached["stamp"] == stamp:
            return cached["view"]
        all_fills, all_markouts = fill_data["rows"], markout_data["rows"]
        session_fills = [
            row
            for row in all_fills
            if not session_id or row.get("session_id") == session_id
        ]
        session_markouts = [
            row
            for row in all_markouts
            if not session_id or row.get("session_id") == session_id
        ]

        session_fill_by_id = {
            str(row.get("fill_id") or ""): row
            for row in session_fills
            if row.get("fill_id")
        }
        markout_30_rows = [
            row
            for row in session_markouts
            if int(row.get("horizon_sec") or 0) == 30
            and str(row.get("fill_id") or "") in session_fill_by_id
        ]
        markout_30_notional = sum(
            float(session_fill_by_id[str(row.get("fill_id"))].get("notional_usd") or 0)
            for row in markout_30_rows
        )
        markout_30_pnl = sum(
            float(row.get("markout_pnl_usd") or 0) for row in markout_30_rows
        )
        markout_30_wear = sum(
            float(row.get("projected_execution_wear_usd") or 0)
            for row in markout_30_rows
        )

        markout_by_fill: dict[str, dict[str, dict]] = {}
        for row in session_markouts:
            fill_id = str(row.get("fill_id") or "")
            horizon = str(row.get("horizon_sec") or "")
            if fill_id and horizon:
                markout_by_fill.setdefault(fill_id, {})[horizon] = row

        enriched = []
        for row in heapq.nlargest(500, session_fills, key=lambda item: int(item.get("time_ms") or 0)):
            item = dict(row)
            marks = markout_by_fill.get(str(row.get("fill_id") or ""), {})
            item["markouts"] = marks
            best = marks.get("30") or marks.get("5") or marks.get("1")
            item["latest_execution_wear_usd"] = (
                best.get("projected_execution_wear_usd")
                if best
                else row.get("projected_booked_wear_usd")
            )
            enriched.append(item)
        enriched.sort(key=lambda row: int(row.get("time_ms") or 0), reverse=True)

        session_buy = sum(
            float(row.get("notional_usd") or 0)
            for row in session_fills
            if row.get("is_buy")
        )
        session_sell = sum(
            float(row.get("notional_usd") or 0)
            for row in session_fills
            if not row.get("is_buy")
        )
        maker_count = sum(
            1
            for row in session_fills
            if str(row.get("liquidity") or "").lower() == "maker"
        )
        view = {
            "session_fills": session_fills,
            "markout_30_rows": markout_30_rows,
            "markout_30_notional": markout_30_notional,
            "markout_30_pnl": markout_30_pnl,
            "markout_30_wear": markout_30_wear,
            "enriched": enriched,
            "session_buy": session_buy,
            "session_sell": session_sell,
            "maker_count": maker_count,
        }
        if len(self.session_cache) >= 32:
            self.session_cache.clear()
        self.session_cache[key] = {"stamp": stamp, "view": view}
        return view


    def build_local_snapshot(self, profile_id: str | None) -> dict:
        deps = self._services()
        available = self.profiles()
        allowed = {item["id"] for item in available}
        if profile_id not in allowed:
            named_matches = (
                [
                    item["id"]
                    for item in available
                    if profile_id and item["id"].endswith(f"/{profile_id}")
                ]
                if profile_id
                else []
            )
            profile_id = (
                named_matches[0]
                if len(named_matches) == 1
                else available[0]["id"] if available else None
            )
        if profile_id is None:
            return {
                "profiles": available,
                "selected": None,
                "summary": None,
                "fills": [],
            }

        summary_path = deps.STATE_DIR / f"{profile_id}_summary.json"
        fills_path = deps.STATE_DIR / f"{profile_id}_fills.jsonl"
        markouts_path = deps.STATE_DIR / f"{profile_id}_markouts.jsonl"
        summary = deps.read_json(summary_path, {})
        profile = next(item for item in available if item["id"] == profile_id)
        summary["dashboard_profile"] = {
            "id": profile_id,
            "mode": profile["mode"],
            "strategy": profile["strategy"],
        }
        session_id = summary.get("session_id")
        view = self._session_ledger_view(fills_path, markouts_path, session_id)
        session_fills = view["session_fills"]
        markout_30_rows = view["markout_30_rows"]
        markout_30_notional = view["markout_30_notional"]
        markout_30_pnl = view["markout_30_pnl"]
        markout_30_wear = view["markout_30_wear"]
        enriched = view["enriched"]
        session_buy = view["session_buy"]
        session_sell = view["session_sell"]
        maker_count = view["maker_count"]
        global_data = [self._jsonl_data(path) for path in deps.STATE_DIR.rglob("*_fills.jsonl")]
        active_portfolio = []
        for item in available:
            if item["mode"] != "live" or item["strategy"] != profile["strategy"]:
                continue
            candidate = deps.read_json(deps.STATE_DIR / f"{item['id']}_summary.json", {})
            if (
                isinstance(candidate, dict)
                and candidate.get("status") == "running"
                and candidate.get("session_started_at_ms")
                and candidate.get("updated_at_ms")
            ):
                active_portfolio.append(candidate)
        portfolio_volume = sum(
            float(item.get("volume_usd") or 0) for item in active_portfolio
        )
        portfolio_started = min(
            (int(item["session_started_at_ms"]) for item in active_portfolio),
            default=0,
        )
        portfolio_updated = max(
            (int(item["updated_at_ms"]) for item in active_portfolio),
            default=0,
        )
        portfolio_elapsed = max((portfolio_updated - portfolio_started) / 1000, 0.0)
        summary["dashboard_aggregates"] = {
            "session_fill_rows": len(session_fills),
            "session_buy_notional_usd": session_buy,
            "session_sell_notional_usd": session_sell,
            "session_maker_ratio": maker_count / len(session_fills)
            if session_fills
            else None,
            "markout_30_samples": len(markout_30_rows),
            "markout_30_observed_notional_usd": markout_30_notional,
            "markout_30_weighted_bps": (
                markout_30_pnl / markout_30_notional * 10_000
                if markout_30_notional > 0
                else None
            ),
            "markout_30_projected_execution_wear_usd": markout_30_wear,
            "markout_30_projected_execution_wear_bps": (
                markout_30_wear / markout_30_notional * 10_000
                if markout_30_notional > 0
                else None
            ),
            "markout_30_positive_ratio": (
                sum(
                    1
                    for row in markout_30_rows
                    if float(row.get("markout_bps") or 0) > 0
                )
                / len(markout_30_rows)
                if markout_30_rows
                else None
            ),
            "markout_30_max_observation_lag_ms": max(
                (int(row.get("observation_lag_ms") or 0) for row in markout_30_rows),
                default=None,
            ),
            "global_fill_rows": sum(len(data["rows"]) for data in global_data),
            "global_volume_usd": sum(
                data["volume"] for data in global_data
            ),
            "global_fees_usd": sum(
                data["fees"] for data in global_data
            ),
            "portfolio_market_count": len(active_portfolio),
            "portfolio_volume_usd": portfolio_volume,
            "portfolio_elapsed_sec": portfolio_elapsed,
            "portfolio_projected_24h_volume_usd": (
                portfolio_volume / portfolio_elapsed * 86_400
                if portfolio_elapsed > 0
                else None
            ),
        }
        return {
            "ok": True,
            "profiles": available,
            "selected": profile_id,
            "summary": summary,
            "fills": enriched[:500],
            "charts": deps.build_charts(session_fills, profile_id=profile_id, session_id=session_id,
                                   wallet_id=str(summary.get("wallet_id") or "primary"),
                                   coin=str(summary.get("coin") or ""), mode=profile["mode"]),
        }


    def build_snapshot(self, profile_id: str | None) -> dict:
        """Backward-compatible local ledger snapshot used by existing tests/tools."""
        value = self.build_local_snapshot(profile_id)
        value.setdefault("ok", True)
        return value



def official_account_view(account_state: dict, spot_state: dict, active_data: dict,
                          *, coin: str, identity: dict) -> dict:
    position = next(
        (
            (item.get("position") or {})
            for item in account_state.get("assetPositions") or []
            if (item.get("position") or {}).get("coin") == coin
        ),
        {},
    )
    position_qty = _number(position.get("szi"))
    spot_usdc = next(
        (
            item
            for item in spot_state.get("balances") or []
            if item.get("coin") == "USDC"
        ),
        {},
    )
    spot_total = _number(spot_usdc.get("total"))
    spot_hold = _number(spot_usdc.get("hold"))
    io_summary = account_state.get("marginSummary") or {}
    io_equity = _number(io_summary.get("accountValue"))
    io_withdrawable = _number(account_state.get("withdrawable"))
    total_io_unrealized = sum(
        _number((item.get("position") or {}).get("unrealizedPnl"))
        for item in account_state.get("assetPositions") or []
    )
    abstraction = str(identity.get("account_abstraction") or "default")
    if abstraction == "unifiedAccount":
        monitored_equity = spot_total + total_io_unrealized
        collateral_source = "unified_spot_usdc"
    else:
        monitored_equity = io_equity
        collateral_source = "io_dex"
    available = active_data.get("availableToTrade") or []
    available_buy = _number(available[0]) if len(available) > 0 else None
    available_sell = _number(available[1]) if len(available) > 1 else None
    free_collateral = (
        min(available_buy, available_sell)
        if available_buy is not None and available_sell is not None
        else io_withdrawable
    )

    return {
        "account_abstraction": abstraction,
        "collateral_source": collateral_source,
        "monitored_equity_usd": monitored_equity,
        "spot_usdc_total": spot_total,
        "spot_usdc_hold": spot_hold,
        "io_account_value": io_equity,
        "io_withdrawable": io_withdrawable,
        "available_to_buy_usd": available_buy,
        "available_to_sell_usd": available_sell,
        "free_collateral_usd": free_collateral,
        "position": {
            "qty": position_qty, "entry_px": _number(position.get("entryPx")),
            "unrealized_pnl_usd": _number(position.get("unrealizedPnl")),
            "liquidation_px": _number(position.get("liquidationPx"))
            if position.get("liquidationPx") not in (None, "") else None,
            "margin_used_usd": _number(position.get("marginUsed")),
            "leverage": position.get("leverage"),
        },
    }


def official_order_rows(open_raw: list, coin: str) -> list[dict]:
    orders = []
    for raw in open_raw:
        if raw.get("coin") != coin:
            continue
        price = _number(raw.get("limitPx") or raw.get("px"))
        remaining = _number(raw.get("sz"))
        original = _number(raw.get("origSz"), remaining)
        is_buy = str(raw.get("side") or "").upper() == "B"
        orders.append(
            {
                "oid": raw.get("oid"),
                "cloid": raw.get("cloid"),
                "is_buy": is_buy,
                "side": "BUY" if is_buy else "SELL",
                "price": price,
                "remaining_qty": remaining,
                "original_qty": original,
                "filled_qty": max(original - remaining, 0.0),
                "notional_usd": price * remaining,
                "timestamp_ms": raw.get("timestamp"),
                "reduce_only": bool(raw.get("reduceOnly")),
                "order_type": raw.get("orderType"),
                "tif": raw.get("tif"),
            }
        )
    orders.sort(key=lambda row: int(row.get("timestamp_ms") or 0), reverse=True)

    return orders


def official_fill_rows(fill_raw: list, coin: str, ownership_by_oid: dict[int, str]) -> list[dict]:
    fills = []
    for raw in fill_raw:
        if raw.get("coin") != coin:
            continue
        price = _number(raw.get("px"))
        qty = _number(raw.get("sz"))
        is_buy = str(raw.get("side") or "").upper() == "B"
        fills.append(
            {
                "time_ms": raw.get("time"),
                "oid": raw.get("oid"),
                "is_buy": is_buy,
                "side": "BUY" if is_buy else "SELL",
                "price": price,
                "qty": qty,
                "notional_usd": price * qty,
                "liquidity": "taker" if raw.get("crossed") else "maker",
                "fee_usd": _number(raw.get("fee")),
                "closed_pnl_usd": _number(raw.get("closedPnl")),
                "direction": raw.get("dir"),
                "ownership": ownership_by_oid.get(
                    int(raw.get("oid") or 0), "unclassified"
                ),
            }
        )
    fills.sort(key=lambda row: int(row.get("time_ms") or 0), reverse=True)
    return fills
