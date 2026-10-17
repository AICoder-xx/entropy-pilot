from __future__ import annotations

import argparse
import asyncio
import ctypes
import heapq
import json
import math
import os
import secrets
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
import webbrowser
from collections import deque
from datetime import datetime
from dataclasses import replace
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, Request, build_opener

import aiohttp
from aiohttp import web

from entropy_bot.broker import order_ownership
from entropy_bot.config import Config, load_config
from entropy_bot.credentials import configured_wallet_ids, resolve_live_credentials, wallet_storage_slug
from entropy_bot.fees import build_fee_model
from entropy_bot.market import MarketClient, build_rest_session
from entropy_bot.storage import atomic_write_json
from dashboard_setup import save_wallet, wallet_status
from dashboard_charts import build_charts
from dashboard_budget import (
    BudgetValidationError, budget_preview, parse_budget_request,
    tuning_budget_conflict, validate_budget_relations,
    validate_complete_payload, values_from_config, with_fixed_budget,
)


ROOT = Path(__file__).resolve().parent
STATE_DIR = ROOT / "state"
LOG_DIR = ROOT / "logs"
INDEX_FILE = ROOT / "dashboard" / "index.html"
LIVE_TEMPLATE = ROOT / "config.live.stage-a.example.json"
RUNTIME_CONFIG = ROOT / "config.live.ui.runtime.json"
CONTROL_FILE = STATE_DIR / "ui_control.json"
BOT_CONSOLE_LOG = LOG_DIR / "ui_bot_console.log"
LIVE_MARKETS = ("SNDK", "ANTH")
# The browser still polls the local endpoint every two seconds so controls and
# ledger summaries feel immediate.  Official Hyperliquid account data is much
# more expensive and is deliberately stale-tolerant while the bots are live:
# local fills/orders remain current, and the dashboard must yield the shared IP
# quota to order and fill reconciliation.
DASHBOARD_ACCOUNT_CACHE_SEC = 180.0
DASHBOARD_FILLS_CACHE_SEC = 300.0
DASHBOARD_FILL_OWNERSHIP_LOOKBACK_SEC = 60 * 60
# A recovery worker may create the control intent immediately before spawning
# the child and then fill in its PID.  The dashboard heartbeat must not call
# that short, legitimate hand-off an unexpected exit.
STARTUP_PID_GRACE_MS = 15_000
AUTO_RECOVERY_DELAY_MS = 30_000
AUTO_RECOVERY_WINDOW_MS = 60 * 60 * 1000
AUTO_RECOVERY_MAX_ATTEMPTS = 3
SAFE_HARD_RECOVERY_MAX_FILLS = 10
SAFE_HARD_RECOVERY_RETRY_MS = 60_000
_JSONL_CACHE: dict[Path, dict] = {}
_SESSION_LEDGER_CACHE: dict[tuple, dict] = {}
GRID_SETTINGS_FILE = ROOT / "grid.settings.json"
DASHBOARD_SETTINGS_FILE = ROOT / "dashboard.config.json"
GRID_SETTING_KEYS = {
    "inventory_direction", "levels_each_side", "spacing_bps",
    "order_notional_usd", "anchor_price",
}


def _is_entropy_dashboard_snapshot(payload: object) -> bool:
    return (
        isinstance(payload, dict)
        and isinstance(payload.get("profiles"), list)
        and isinstance(payload.get("controller"), dict)
        and "ok" in payload
    )


def _existing_entropy_dashboard(port: int, *, timeout: float = 0.8, required_read_only: bool | None = None) -> bool:
    """Recognize an existing local panel without using system proxy settings."""

    request = Request(
        f"http://127.0.0.1:{port}/api/snapshot",
        headers={"User-Agent": "EntropyDashboardLauncher/1"},
    )
    opener = build_opener(ProxyHandler({}))
    try:
        with opener.open(request, timeout=timeout) as response:
            if response.status != 200:
                return False
            payload = json.loads(response.read(2 * 1024 * 1024).decode("utf-8"))
    except (HTTPError, URLError, OSError, UnicodeDecodeError, json.JSONDecodeError):
        return False
    recognized = _is_entropy_dashboard_snapshot(payload)
    if recognized and required_read_only is not None and bool(payload.get("read_only")) != required_read_only:
        raise RuntimeError(f"端口 {port} 上已有不同模式的 Entropy 面板；请用 --port 8767 等空闲端口")
    return recognized


def _safe_reconciliation_error_label(exc: Exception) -> str:
    if type(exc).__name__ == "MissingRecoveryAccountError":
        return "新目录缺少有效账户配置"
    return type(exc).__name__

_SAFE_TRANSIENT_HALT_HINTS = (
    "Cannot connect to host",
    "ClientConnectorError",
    "ServerDisconnectedError",
    "TimeoutError",
    "官方连接中断",
    # Exchange-view flicker: stalled cancellation confirmation must be
    # reconciled against the official book before quoting can resume.
    # These strings must remain substrings of the RuntimeError text in
    # entropy_bot/broker.py; test_dashboard checks the exact coupling.
    "笔撤单超过",
    "撤销后官方视图持续矛盾",
    # A place whose outcome could not be confirmed even by cloid probes.  Its
    # generated cloid remains a fail-closed recovery identity across restart;
    # unattended retry is never allowed from a transient unknownOid lookup.
    "下单结果不确定",
)

_HARD_RECOVERY_BLOCK_HINTS = (
    "风险停机",
    "启动前风险检查失败",
    "本金累计回撤",
    "实际权益回撤",
    "低于保留线",
    "钱包私钥格式无效",
    "账户角色",
    "拒绝签名",
    "杠杆核验失败",
    "Growth Mode",
    "配置错误",
)


async def reconcile_stopped_market(
    symbol: str, *, wallet_id: str = "primary", apply: bool, max_fills: int
) -> dict:
    """Lazy import keeps the dashboard startup path lightweight."""

    from recover_stopped_fills import reconcile

    return await reconcile(
        symbol,
        wallet_id=wallet_id,
        apply=apply,
        max_fills=max_fills,
    )


def _safe_slug(value: str) -> str:
    return wallet_storage_slug(value)


def market_runtime_paths(
    symbol: str, wallet_id: str = "primary"
) -> tuple[Path, Path, Path]:
    slug = symbol.lower()
    wallet_slug = _safe_slug(wallet_id)
    if wallet_slug != "primary":
        slug = f"{wallet_slug}.{slug}"
    return (
        ROOT / f"config.live.ui.{slug}.runtime.json",
        STATE_DIR / f"ui_control_{slug}.json",
        LOG_DIR / f"ui_bot_{slug}_console.log",
    )


def _merge_dict(target: dict, updates: dict) -> None:
    for key, value in updates.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            _merge_dict(target[key], value)
        else:
            target[key] = value


# ---------------------------------------------------------------------------
# AI-operator tuning overlay
#
# tuning.overrides.json is the ONLY file the autonomous operator is allowed to
# edit for parameter tuning.  Every key is whitelisted and every value is
# clamped to a reviewed safety range before it can reach a live config, so no
# edit of that file can escape the risk envelope.  Risk limits, leverage,
# drawdown lines, fail-closed timeouts and stop-loss behavior are deliberately
# NOT tunable here.  A malformed file degrades to "no overrides", which is the
# safe direction.
# ---------------------------------------------------------------------------

TUNING_FILE = ROOT / "tuning.overrides.json"
TUNING_BOUNDS: dict[str, tuple[float, float]] = {
    "market_maker.half_spread_bps": (0.8, 5.0),
    "market_maker.refresh_bps": (0.5, 2.0),
    "market_maker.refresh_sec": (0.5, 5.0),
    "market_maker.min_edge_after_projected_rebate_bps": (0.3, 2.0),
    "market_maker.inventory_skew_bps_at_limit": (1.0, 6.0),
    "market_maker.adaptive_spread_max_extra_bps": (1.0, 4.0),
    "market_maker.volatility_guard_range_bps": (10.0, 60.0),
    "market_maker.same_side_fill_cooldown_sec": (0.0, 30.0),
    "market_maker.crisis_quote_notional_usd": (0.0, 800.0),
    "market_maker.crisis_quote_distance_bps": (30.0, 300.0),
    "market_maker.level_spacing_bps": (2.0, 10.0),
    "market_maker.vol_spread_mult": (0.0, 3.0),
    "market_maker.vol_skew_mult": (0.0, 3.0),
    "market_maker.microprice_weight": (0.0, 1.0),
    "market_maker.aggressive_requote_multiplier": (1.0, 6.0),
    "market_maker.aggressive_requote_min_age_sec": (0.0, 10.0),
    "market_maker.adaptive_spread_idle_decay_sec": (600.0, 7200.0),
    "runtime.budget_soft_remaining": (2000, 10000),
    "runtime.budget_hard_remaining": (800, 3000),
    "runtime.reserve_actions_daily_max_usd": (0.0, 50.0),
}
_TUNING_INT_KEYS = {
    "runtime.budget_soft_remaining",
    "runtime.budget_hard_remaining",
}


def load_tuning_overrides(symbol: str) -> tuple[dict, dict]:
    """Read the sanitized per-market tuning overlay.

    Returns ``(nested_overrides, rejected)``.  ``rejected`` maps offending
    keys to a human-readable reason (unknown key, non-numeric, clamped).
    """
    payload = read_json(TUNING_FILE, None)
    if not isinstance(payload, dict):
        return {}, {}
    section = payload.get(symbol.upper())
    if not isinstance(section, dict):
        return {}, {}
    nested: dict = {}
    rejected: dict[str, str] = {}
    for key, raw in section.items():
        name = str(key)
        if name.startswith("_"):
            continue
        bounds = TUNING_BOUNDS.get(name)
        if bounds is None:
            rejected[name] = "不在白名单，已忽略"
            continue
        try:
            value = float(raw)
        except (TypeError, ValueError):
            rejected[name] = "不是数值，已忽略"
            continue
        low, high = bounds
        clamped = min(max(value, low), high)
        if clamped != value:
            rejected[name] = f"越界，已夹紧到 {clamped}"
        if name in _TUNING_INT_KEYS:
            clamped = int(round(clamped))
        head, _, tail = name.partition(".")
        nested.setdefault(head, {})[tail] = clamped
    return nested, rejected


def apply_tuning_overrides(payload: dict, symbol: str) -> None:
    """Merge the sanitized tuning overlay into a runtime config payload.

    Also keeps the budget threshold pair consistent (config validation
    requires hard < soft) so a partial edit can never block a start.
    The applied/rejected report is embedded in the payload for auditing.
    """
    tuning, rejected = load_tuning_overrides(symbol)
    if tuning:
        _merge_dict(payload, tuning)
        runtime_cfg = payload.get("runtime")
        if isinstance(runtime_cfg, dict):
            soft = runtime_cfg.get("budget_soft_remaining")
            hard = runtime_cfg.get("budget_hard_remaining")
            if (
                isinstance(soft, (int, float))
                and isinstance(hard, (int, float))
                and hard >= soft
            ):
                runtime_cfg["budget_hard_remaining"] = int(max(800, soft * 0.3))
                rejected["runtime.budget_hard_remaining"] = (
                    "与 soft 阈值冲突，已重设为 soft 的 30%"
                )
    payload["_tuning_applied"] = tuning
    payload["_tuning_rejected"] = rejected


def read_json(path: Path, default):
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError):
        return default


def read_jsonl(path: Path) -> list[dict]:
    return list(_jsonl_data(path)["rows"])


def _jsonl_data(path: Path) -> dict:
    """Incrementally read complete lines and retain cheap lifetime totals."""
    cached = _JSONL_CACHE.setdefault(path, {"offset": 0, "rows": [], "volume": 0.0, "fees": 0.0})
    try:
        stat = path.stat()
        size = stat.st_size
        identity = (stat.st_dev, stat.st_ino)
        if (size < cached["offset"] or cached.get("identity", identity) != identity
                or (size == cached.get("size") and stat.st_mtime_ns != cached.get("mtime_ns"))):
            cached = {"offset": 0, "rows": [], "volume": 0.0, "fees": 0.0}
            _JSONL_CACHE[path] = cached
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


def profiles() -> list[dict]:
    result = []
    for path in STATE_DIR.rglob("*_summary.json"):
        summary = read_json(path, {})
        if not isinstance(summary, dict):
            continue
        relative = path.relative_to(STATE_DIR).as_posix()
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


def _session_ledger_view(fills_path: Path, markouts_path: Path, session_id: str | None) -> dict:
    fill_data, markout_data = _jsonl_data(fills_path), _jsonl_data(markouts_path)
    key = (fills_path, markouts_path, session_id)
    stamp = tuple((id(data), data.get("identity"), data.get("mtime_ns"), data["offset"]) for data in (fill_data, markout_data))
    cached = _SESSION_LEDGER_CACHE.get(key)
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
    if len(_SESSION_LEDGER_CACHE) >= 32:
        _SESSION_LEDGER_CACHE.clear()
    _SESSION_LEDGER_CACHE[key] = {"stamp": stamp, "view": view}
    return view


def build_local_snapshot(profile_id: str | None) -> dict:
    available = profiles()
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

    summary_path = STATE_DIR / f"{profile_id}_summary.json"
    fills_path = STATE_DIR / f"{profile_id}_fills.jsonl"
    markouts_path = STATE_DIR / f"{profile_id}_markouts.jsonl"
    summary = read_json(summary_path, {})
    profile = next(item for item in available if item["id"] == profile_id)
    summary["dashboard_profile"] = {
        "id": profile_id,
        "mode": profile["mode"],
        "strategy": profile["strategy"],
    }
    session_id = summary.get("session_id")
    view = _session_ledger_view(fills_path, markouts_path, session_id)
    session_fills = view["session_fills"]
    markout_30_rows = view["markout_30_rows"]
    markout_30_notional = view["markout_30_notional"]
    markout_30_pnl = view["markout_30_pnl"]
    markout_30_wear = view["markout_30_wear"]
    enriched = view["enriched"]
    session_buy = view["session_buy"]
    session_sell = view["session_sell"]
    maker_count = view["maker_count"]
    global_data = [_jsonl_data(path) for path in STATE_DIR.rglob("*_fills.jsonl")]
    active_portfolio = []
    for item in available:
        if item["mode"] != "live" or item["strategy"] != profile["strategy"]:
            continue
        candidate = read_json(STATE_DIR / f"{item['id']}_summary.json", {})
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
        "charts": build_charts(session_fills, profile_id=profile_id, session_id=session_id,
                               wallet_id=str(summary.get("wallet_id") or "primary"),
                               coin=str(summary.get("coin") or ""), mode=profile["mode"]),
    }


def build_snapshot(profile_id: str | None) -> dict:
    """Backward-compatible local ledger snapshot used by existing tests/tools."""
    value = build_local_snapshot(profile_id)
    value.setdefault("ok", True)
    return value


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


async def classify_recent_fill_ownership(
    market: MarketClient,
    address: str,
    raw_rows: list[dict],
    *,
    coin: str,
    cache: dict[int, str],
    now_ms: int,
    lookback_sec: int = DASHBOARD_FILL_OWNERSHIP_LOOKBACK_SEC,
) -> dict[int, str]:
    """Classify only recent stopped-market fills, one request per order."""
    all_oids = {
        int(raw.get("oid") or 0)
        for raw in raw_rows
        if raw.get("coin") == coin and int(raw.get("oid") or 0) > 0
    }
    for oid in list(cache):
        if oid not in all_oids:
            cache.pop(oid, None)

    cutoff_ms = now_ms - max(lookback_sec, 0) * 1000
    recent_oids = {
        int(raw.get("oid") or 0)
        for raw in raw_rows
        if raw.get("coin") == coin
        and int(raw.get("time") or 0) >= cutoff_ms
        and int(raw.get("oid") or 0) > 0
    }
    result: dict[int, str] = {}
    for oid in sorted(recent_oids):
        cached = cache.get(oid)
        if cached in {"bot", "external"}:
            result[oid] = cached
            continue
        try:
            status = await market.order_status(address, oid)  # type: ignore[arg-type]
            ownership, _ = order_ownership(status)
        except Exception:
            ownership = "unknown"
        result[oid] = ownership
        if ownership in {"bot", "external"}:
            cache[oid] = ownership
    return result


class LiveMonitor:
    class WalletNotConfigured(RuntimeError):
        """Expected first-run state; never contains credential details."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        symbol: str = "SNDK",
        wallet_id: str = "primary",
    ) -> None:
        self.session = session
        self.symbol = symbol.upper()
        self.wallet_id = wallet_id
        self.cfg: Config | None = None
        self.market: MarketClient | None = None
        self.address: str | None = None
        self.credentials_source = "none"
        self.address_derived = False
        self._cache: dict | None = None
        self._cache_at = 0.0
        self._identity_cache: dict | None = None
        self._identity_at = 0.0
        self._fills_cache: list = []
        self._fills_at = 0.0
        self._fills_updated_at_ms = 0
        self._fills_initialized = False
        self._fill_ownership_cache: dict[int, str] = {}
        self._lock = asyncio.Lock()

    async def _initialize(self) -> None:
        if self.cfg is not None:
            return
        base = load_config(LIVE_TEMPLATE)
        cfg = replace(
            base,
            market=replace(base.market, symbol=self.symbol),
            live=replace(base.live, wallet_id=self.wallet_id),
        )
        credentials = resolve_live_credentials(
            cfg, require_private_key=False
        )
        if not credentials.account_address:
            raise self.WalletNotConfigured()
        # SNDK and ANTH monitors are independent clients in this process.  A
        # measured cold monitor used 246 weight for metadata, identity and its
        # first snapshot. 250 avoids waiting a full local rate-limit window;
        # the UI uses local telemetry instead of this monitor while bots run.
        market = MarketClient(cfg.market, self.session, info_capacity=250)
        await market.load_meta()
        # Commit only after all initialization steps succeed. Failed first-run
        # setup or metadata requests must leave the next refresh free to retry.
        self.cfg = cfg
        self.market = market
        self.address = credentials.account_address
        self.credentials_source = credentials.source
        self.address_derived = credentials.account_was_derived

    async def _identity(self) -> dict:
        assert self.market and self.address and self.cfg
        if self._identity_cache and time.monotonic() - self._identity_at < 600:
            return self._identity_cache
        role, abstraction, fees = await asyncio.gather(
            self.market.user_role(self.address),
            self.market.user_abstraction(self.address),
            self.market.user_fees(self.address),
        )
        fee_model = build_fee_model(
            self.market.meta,
            fees,
            self_rebate_pct=self.cfg.fees.entropy_self_rebate_pct,
            referred_benefit_pct=self.cfg.fees.entropy_referred_user_benefit_pct,
        )
        self._identity_cache = {
            "role": role.get("role"),
            "account_abstraction": abstraction,
            "fee_model": fee_model.to_dict(),
        }
        self._identity_at = time.monotonic()
        return self._identity_cache

    async def snapshot(self, *, force: bool = False) -> dict:
        if not force and self._cache is not None:
            if (
                time.monotonic() - self._cache_at < DASHBOARD_ACCOUNT_CACHE_SEC
                or self._lock.locked()
            ):
                return self._cache
        async with self._lock:
            if (
                not force
                and self._cache is not None
                and time.monotonic() - self._cache_at
                < DASHBOARD_ACCOUNT_CACHE_SEC
            ):
                return self._cache
            try:
                await self._initialize()
                assert self.market and self.address and self.cfg
                identity = await self._identity()
                refresh_fills = (
                    not self._fills_initialized
                    or time.monotonic() - self._fills_at
                    >= DASHBOARD_FILLS_CACHE_SEC
                )
                fill_request = (
                    self.market.fills_since(
                        self.address, int((time.time() - 24 * 3600) * 1000)
                    )
                    if refresh_fills
                    else asyncio.sleep(0, result=self._fills_cache)
                )
                (
                    market_snapshot,
                    account_state,
                    spot_state,
                    active_data,
                    open_raw,
                    fill_raw,
                ) = await asyncio.gather(
                    self.market.refresh_context(),
                    self.market.account_state(self.address),
                    self.market.spot_state(self.address),
                    self.market.active_asset_data(self.address),
                    self.market.open_orders(self.address),
                    fill_request,
                )
                if refresh_fills:
                    self._fills_cache = list(fill_raw)
                    self._fills_at = time.monotonic()
                    self._fills_updated_at_ms = int(time.time() * 1000)
                    self._fills_initialized = True
                await self.market.rest_book_once()
                coin = self.market.meta.coin
                ownership_by_oid = await classify_recent_fill_ownership(
                    self.market,
                    self.address,
                    list(fill_raw),
                    coin=coin,
                    cache=self._fill_ownership_cache,
                    now_ms=int(time.time() * 1000),
                )
                mark_px = _number(market_snapshot.mark_px)
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
                exposure = calculate_exposure(position_qty, mark_px, orders)
                self._cache = {
                    "ok": True,
                    "updated_at_ms": int(time.time() * 1000),
                    "coin": coin,
                    "role": identity.get("role"),
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
                        "qty": position_qty,
                        "entry_px": _number(position.get("entryPx")),
                        "unrealized_pnl_usd": _number(position.get("unrealizedPnl")),
                        "liquidation_px": _number(position.get("liquidationPx"))
                        if position.get("liquidationPx") not in (None, "")
                        else None,
                        "margin_used_usd": _number(position.get("marginUsed")),
                        "leverage": position.get("leverage"),
                    },
                    "market": {
                        "best_bid": market_snapshot.book.best_bid,
                        "best_ask": market_snapshot.book.best_ask,
                        "spread_bps": market_snapshot.book.spread_bps,
                        "mark_px": market_snapshot.mark_px,
                        "oracle_px": market_snapshot.oracle_px,
                        "funding": market_snapshot.funding,
                        "day_notional_volume": market_snapshot.day_notional_volume,
                        "growth_mode": self.market.meta.growth_mode,
                        "isolated_only": self.market.meta.only_isolated,
                    },
                    "exposure": exposure,
                    "open_orders": orders,
                    "recent_fills": fills[:200],
                    # Account/open-order snapshots refresh more often than the
                    # deliberately low-rate userFills cache.  Expose the
                    # independent freshness boundary so audits never compare
                    # new local fills against an older official fill page.
                    "fills_updated_at_ms": self._fills_updated_at_ms,
                    "fee_model": identity.get("fee_model"),
                }
                self._cache_at = time.monotonic()
                return self._cache
            except self.WalletNotConfigured:
                return {
                    "ok": False,
                    "updated_at_ms": int(time.time() * 1000),
                    "code": "wallet_not_configured",
                    "error": "尚未配置钱包，请在“钱包与配置”填写资金账户和已授权的 API 钱包私钥。",
                }
            except Exception as exc:
                return {
                    "ok": False,
                    "updated_at_ms": int(time.time() * 1000),
                    "error": f"{type(exc).__name__}: 只读账户核验失败",
                }


def _pid_exists(pid: int | None) -> bool:
    if not pid or pid <= 0:
        return False
    if os.name == "nt":
        # Windows' os.kill(pid, 0) is not a POSIX-style existence probe: Python
        # routes ordinary signals through TerminateProcess. Query the process
        # handle instead so a dashboard status refresh can never kill the bot.
        process_query_limited_information = 0x1000
        still_active = 259
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        open_process = kernel32.OpenProcess
        open_process.argtypes = [ctypes.c_ulong, ctypes.c_int, ctypes.c_ulong]
        open_process.restype = ctypes.c_void_p
        get_exit_code = kernel32.GetExitCodeProcess
        get_exit_code.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_ulong)]
        get_exit_code.restype = ctypes.c_int
        close_handle = kernel32.CloseHandle
        close_handle.argtypes = [ctypes.c_void_p]
        close_handle.restype = ctypes.c_int
        handle = open_process(process_query_limited_information, 0, pid)
        if not handle:
            return False
        try:
            exit_code = ctypes.c_ulong()
            if not get_exit_code(handle, ctypes.byref(exit_code)):
                return False
            return exit_code.value == still_active
        finally:
            close_handle(handle)
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


class BotController:
    def __init__(
        self,
        monitor: LiveMonitor,
        *,
        symbol: str = "SNDK",
        runtime_config: Path | None = None,
        control_file: Path | None = None,
        console_log: Path | None = None,
        runtime_overrides: dict | None = None,
        wallet_id: str = "primary",
    ) -> None:
        self.monitor = monitor
        self.symbol = symbol.upper()
        self.wallet_id = wallet_id
        self.runtime_config = runtime_config or RUNTIME_CONFIG
        self.control_file = control_file or CONTROL_FILE
        self.console_log = console_log or BOT_CONSOLE_LOG
        self.runtime_overrides = runtime_overrides or {}
        self.process: subprocess.Popen | None = None
        self._lock = asyncio.Lock()
        self._recovering = False
        self._hard_recovering = False
        self._recovery_attempts_ms: deque[int] = deque()

    def _load_recovery_attempts(self, now_ms: int | None = None) -> deque[int]:
        """Merge the persisted rolling window with this process' memory.

        The dashboard itself may be restarted by Windows or an external
        supervisor.  Keeping the retry window only in memory would reset the
        circuit breaker and could turn a persistent fault into a restart loop.
        """

        now_ms = now_ms or int(time.time() * 1000)
        value = read_json(self.control_file, {})
        persisted = value.get("auto_recovery_attempts_ms", []) if isinstance(value, dict) else []
        candidates: set[int] = set(self._recovery_attempts_ms)
        if isinstance(persisted, list):
            for item in persisted:
                try:
                    attempt_ms = int(item)
                except (TypeError, ValueError, OverflowError):
                    continue
                if attempt_ms > 0:
                    candidates.add(attempt_ms)
        self._recovery_attempts_ms = deque(
            sorted(
                attempt_ms
                for attempt_ms in candidates
                if now_ms - attempt_ms <= AUTO_RECOVERY_WINDOW_MS
            )
        )
        return self._recovery_attempts_ms

    def _put_recovery_attempts(self, value: dict, now_ms: int | None = None) -> None:
        attempts = self._load_recovery_attempts(now_ms)
        value["auto_recovery_attempts_ms"] = list(attempts)
        value["auto_recovery_count"] = len(attempts)

    def status(self) -> dict:
        value = read_json(self.control_file, {})
        if not isinstance(value, dict):
            value = {}
        attempts = self._load_recovery_attempts()
        pid = int(value.get("pid") or 0)
        running = _pid_exists(pid)
        risk_profile = value.get("risk_profile")
        profile_source = "last_run"
        if not isinstance(risk_profile, dict):
            planned = read_json(LIVE_TEMPLATE, {})
            _merge_dict(planned, self.runtime_overrides)
            apply_tuning_overrides(planned, self.symbol)
            risk = planned.get("risk") or {}
            risk_profile = {
                "configured_capital_usd": risk.get("capital_usd"),
                "order_notional_usd": (planned.get("market_maker") or {}).get("order_notional_usd"),
                "max_position_usd": risk.get("max_position_usd"),
                "max_open_order_usd": risk.get("max_open_order_usd"),
                "max_drawdown_usd": risk.get("max_session_drawdown_usd"),
                "max_principal_drawdown_usd": risk.get("max_principal_drawdown_usd"),
            }
            profile_source = "planned"
        return {
            "running": running,
            "desired_state": value.get("desired_state") or "stopped",
            "process_state": value.get("process_state")
            or ("running" if running else "stopped"),
            "strategy": value.get("strategy"),
            "run_id": value.get("run_id"),
            "pid": pid if running else None,
            "started_at_ms": value.get("started_at_ms"),
            "last_exit_code": value.get("last_exit_code"),
            "message": value.get("message"),
            "risk_profile": risk_profile,
            "risk_profile_source": profile_source,
            "symbol": self.symbol,
            "wallet_id": self.wallet_id,
            "grid_settings": self.grid_settings(),
            "auto_recovery_count": len(attempts),
            "recovery_hold_until_session_pause": (
                value.get("recovery_hold_until_session_pause") is True
            ),
            "recovery_hold_released_at_ms": value.get(
                "recovery_hold_released_at_ms"
            ),
        }

    def grid_settings(self) -> dict:
        template = read_json(LIVE_TEMPLATE, {})
        settings = {key: value for key, value in (template.get("grid") or {}).items() if key in GRID_SETTING_KEYS}
        settings.setdefault("inventory_direction", "long")
        settings.update({key: value for key, value in self.runtime_overrides.get("grid", {}).items() if key in GRID_SETTING_KEYS})
        saved = read_json(GRID_SETTINGS_FILE, {})
        entry = saved.get(f"{self.wallet_id}:{self.symbol}", {}) if isinstance(saved, dict) else {}
        if isinstance(entry, dict):
            settings.update({key: value for key, value in entry.items() if key in GRID_SETTING_KEYS})
        return settings

    def _grid_state_dir(self) -> Path:
        base = Path(self.runtime_overrides.get("runtime", {}).get("state_dir") or "state")
        return (base if base.is_absolute() else ROOT / base) / "strategies" / "grid"

    def _guard_grid_direction_change(self) -> None:
        """Inspect the durable book read-only, never construct/mutate it here."""
        folder, slug = self._grid_state_dir(), f"live_grid_io_{self.symbol}"
        database = folder / f"{slug}_inventory.sqlite3"
        if database.exists():
            try:
                connection = sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True, timeout=1)
                try:
                    row = connection.execute("SELECT value FROM accounting_state WHERE id=1").fetchone()
                    state = json.loads(row[0]) if row else None
                finally:
                    connection.close()
                if not isinstance(state, dict):
                    raise ValueError("missing inventory state")
                qty = Decimal(str(state["qty"]))
                if not qty.is_finite() or qty != 0 or state.get("lots") or state.get("fatal_reason") or state.get("reconciliation_reason"):
                    raise ValueError("unreconciled inventory")
            except (sqlite3.Error, ValueError, KeyError, TypeError, OSError, OverflowError, InvalidOperation):
                raise web.HTTPConflict(text=f"方向未修改：真实库存台账未证明为空仓或需要恢复，已保留 {database}") from None
        orders_file = folder / f"{slug}_state.json"
        if orders_file.exists():
            saved = read_json(orders_file, None)
            if not isinstance(saved, dict) or saved.get("orders"):
                raise web.HTTPConflict(text=f"方向未修改：本地尚有未完成或未确认委托，已保留 {orders_file}")

    def _is_pristine_grid_setup(self) -> bool:
        """A config default is not an existing strategy's inventory direction.

        Permit first adoption of matching exchange inventory only when no grid
        history exists, or a read-only inspection proves the book has never
        initialized or recorded any economic event. Never clear old errors.
        """
        folder, slug = self._grid_state_dir(), f"live_grid_io_{self.symbol}"
        database = folder / f"{slug}_inventory.sqlite3"
        artifacts = list(folder.glob(f"{slug}*"))
        if not database.exists():
            return not artifacts
        allowed = {
            f"{slug}{suffix}" for suffix in (
                "_inventory.sqlite3", "_inventory.sqlite3-wal", "_inventory.sqlite3-shm",
                "_summary.json", "_state.json", "_orders.jsonl", "_fills.jsonl", "_markouts.jsonl",
            )
        }
        if any(path.name not in allowed for path in artifacts):
            return False
        try:
            connection = sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True, timeout=1)
            try:
                connection.execute("BEGIN")
                row = connection.execute("SELECT value FROM accounting_state WHERE id=1").fetchone()
                events = connection.execute("SELECT COUNT(*) FROM accounting_events").fetchone()[0]
                state = json.loads(row[0]) if row else None
            finally:
                connection.close()
            if (not isinstance(state, dict) or state.get("initialized") is not False
                    or events != 0 or state.get("lots") != []
                    or state.get("fatal_reason") is not None
                    or state.get("reconciliation_reason") is not None):
                return False
            for key in ("qty", "entry", "initial_qty", "initial_entry", "realized", "fees", "funding",
                        "pair_gross", "pair_fees", "matched_qty", "discarded_pair_qty", "fill_count", "funding_count"):
                value = Decimal(str(state[key]))
                if not value.is_finite() or value != 0:
                    return False
            for suffix in ("_orders.jsonl", "_fills.jsonl", "_markouts.jsonl"):
                path = folder / f"{slug}{suffix}"
                if path.exists() and path.stat().st_size:
                    return False
            path = folder / f"{slug}_state.json"
            if path.exists():
                saved = read_json(path, None)
                if not isinstance(saved, dict) or saved.get("orders") or (saved.get("strategy") or {}).get("blocked"):
                    return False
            path = folder / f"{slug}_summary.json"
            if path.exists():
                summary = read_json(path, None)
                if not isinstance(summary, dict):
                    return False
                accounting = summary.get("grid_accounting") or {}
                if (accounting.get("initialized") is not False
                        or accounting.get("status") != "uninitialized"
                        or Decimal(str(accounting.get("inventory_qty"))) != 0
                        or summary.get("open_order_details") or _number(summary.get("open_orders"))):
                    return False
            return True
        except (sqlite3.Error, ValueError, KeyError, TypeError, AttributeError, OSError, OverflowError, InvalidOperation):
            return False

    def _validate_grid_settings(self, updates: object) -> dict:
        if GRID_SETTINGS_FILE.exists() and not isinstance(read_json(GRID_SETTINGS_FILE, None), dict):
            raise web.HTTPConflict(text="网格配置文件损坏，已保留原文件，请修复后重试")
        if not isinstance(updates, dict) or set(updates) - GRID_SETTING_KEYS:
            raise web.HTTPBadRequest(text="网格参数包含不支持的字段")
        value = {**self.grid_settings(), **updates}
        if value.get("inventory_direction") not in {"long", "short"}:
            raise web.HTTPBadRequest(text="库存方向只能选择 long 或 short")
        bounds = {"levels_each_side": (1, 100), "spacing_bps": (0.1, 2000), "order_notional_usd": (10, 1800)}
        for key, (lower, upper) in bounds.items():
            try:
                raw = value[key]
                number = float(raw)
                if isinstance(raw, bool) or not math.isfinite(number) or not lower <= number <= upper:
                    raise ValueError
                if key == "levels_each_side" and number != int(number):
                    raise ValueError
            except (KeyError, TypeError, ValueError, OverflowError):
                raise web.HTTPBadRequest(text=f"{key} 必须在 {lower} 至 {upper} 之间") from None
            value[key] = int(number) if key == "levels_each_side" else number
        if value["levels_each_side"] * value["spacing_bps"] >= 9900:
            raise web.HTTPBadRequest(text="层数 × 间距必须小于 9900 bps")
        anchor = value.get("anchor_price")
        try:
            anchor = None if anchor in (None, "") else float(anchor)
            if anchor is not None and (not math.isfinite(anchor) or anchor <= 0):
                raise ValueError
        except (ValueError, TypeError, OverflowError):
            raise web.HTTPBadRequest(text="锚点价格须大于 0，留空表示自动") from None
        value["anchor_price"] = anchor
        return value

    async def save_grid_settings(self, updates: object) -> dict:
        async with self._lock:
            if self.status()["running"]:
                raise web.HTTPConflict(text="请先停止该钱包、市场的策略再修改网格参数")
            value = self._validate_grid_settings(updates)
            if value["inventory_direction"] != self.grid_settings().get("inventory_direction"):
                # First adoption may match an existing exchange position. An
                # established grid can change direction only after going flat.
                live = await self.monitor.snapshot(force=True)
                try:
                    quantity = float(live["position"]["qty"])
                    position_known = math.isfinite(quantity)
                except (KeyError, TypeError, ValueError, OverflowError):
                    quantity, position_known = 0.0, False
                if not live.get("ok") or not position_known or not isinstance(live.get("open_orders"), list):
                    raise web.HTTPConflict(text="方向未修改：无法核验交易所真实持仓，请恢复账户连接后重试")
                if live.get("open_orders"):
                    raise web.HTTPConflict(text="方向未修改：该市场有未完成委托；首次接管和已有网格变向都须先确认撤单完成")
                first_adoption = self._is_pristine_grid_setup()
                if quantity != 0:
                    if not first_adoption:
                        raise web.HTTPConflict(text="方向未修改：已有网格账本、历史或未核清状态，不能带仓变向；请先对账、平仓并确认撤单完成")
                    expected_sign = 1 if value["inventory_direction"] == "long" else -1
                    if quantity * expected_sign < 0:
                        raise web.HTTPConflict(text="方向未修改：首次接管所选方向必须与交易所现有仓位一致；做多接管多仓，做空接管空仓")
                self._guard_grid_direction_change()
                summary = read_json(self._grid_state_dir() / f"live_grid_io_{self.symbol}_summary.json", {})
                accounting = summary.get("grid_accounting") or {}
                if (_number(accounting.get("inventory_qty")) != 0
                        or summary.get("open_order_details") or _number(summary.get("open_orders"))
                        or accounting.get("status") in {"unknown", "reconciliation_required", "error"}):
                    raise web.HTTPConflict(text="方向未修改：库存台账尚未对账为空仓，或存在未确认委托")
            saved = read_json(GRID_SETTINGS_FILE, None) if GRID_SETTINGS_FILE.exists() else {}
            if not isinstance(saved, dict):
                raise web.HTTPConflict(text="网格配置文件损坏，已保留原文件，请修复后重试")
            saved[f"{self.wallet_id}:{self.symbol}"] = value
            atomic_write_json(GRID_SETTINGS_FILE, saved)
            return value

    async def start(
        self,
        strategy: str,
        *,
        recovery_hold_until_session_pause: bool = False,
    ) -> dict:
        if strategy not in {"market_maker", "grid"}:
            raise web.HTTPBadRequest(text="策略只能是 market_maker 或 grid")
        async with self._lock:
            current = self.status()
            if current["running"]:
                raise web.HTTPConflict(text="已有机器人进程在运行")
            await self._gate_previous_session_summary()

            payload = read_json(LIVE_TEMPLATE, None)
            if not isinstance(payload, dict):
                raise web.HTTPInternalServerError(text="实盘模板不可读")
            payload["strategy"] = strategy
            _merge_dict(payload, self.runtime_overrides)
            if strategy == "grid":
                payload.setdefault("grid", {}).update(self._validate_grid_settings({}))
                runtime = payload.setdefault("runtime", {})
                runtime["state_dir"] = str(self._grid_state_dir())
                runtime["log_dir"] = str(Path(runtime.get("log_dir") or "logs") / "strategies" / "grid")
                payload.setdefault("risk", {})["auto_scale_with_equity"] = False
            # AI-operator tuning overlay: whitelisted, clamped, last-writer.
            apply_tuning_overrides(payload, self.symbol)
            payload.setdefault("live", {})["wallet_id"] = self.wallet_id
            payload["_说明"] = (
                "由本地控制面板生成；不含私钥。修改模板后重新启动即可生效。"
            )
            atomic_write_json(self.runtime_config, payload)

            # Do not duplicate the bot's account/role/margin REST preflight in
            # the dashboard.  A cold monitor costs roughly 246 info weight per
            # market and can delay startup exactly when the shared IP quota is
            # busy.  Each bot independently verifies credentials, account role,
            # abstraction, leverage, fees and initial risk before any order.
            risk = payload.get("risk") or {}

            run_id = uuid.uuid4().hex
            now_ms = int(time.time() * 1000)
            control = {
                "run_id": run_id,
                "summary_session_id": run_id,
                "desired_state": "running",
                "process_state": "starting",
                "dashboard_heartbeat_ms": now_ms,
                "strategy": strategy,
                "symbol": self.symbol,
                "wallet_id": self.wallet_id,
                "started_at_ms": now_ms,
                "pid": None,
                "message": (
                    "正在启动恢复隔离态（仅监控、禁止挂单）"
                    if recovery_hold_until_session_pause
                    else "正在启动"
                ),
                "recovery_hold_until_session_pause": bool(
                    recovery_hold_until_session_pause
                ),
                "risk_profile": {
                    "configured_capital_usd": risk.get("capital_usd"),
                    "order_notional_usd": (
                        payload.get(strategy) or {}
                    ).get("order_notional_usd"),
                    "max_position_usd": risk.get("max_position_usd"),
                    "max_open_order_usd": risk.get("max_open_order_usd"),
                    "max_drawdown_usd": risk.get("max_session_drawdown_usd"),
                    "max_principal_drawdown_usd": risk.get(
                        "max_principal_drawdown_usd"
                    ),
                },
            }
            self._put_recovery_attempts(control, now_ms)
            STATE_DIR.mkdir(parents=True, exist_ok=True)
            LOG_DIR.mkdir(parents=True, exist_ok=True)
            atomic_write_json(self.control_file, control)
            creation_flags = 0
            if os.name == "nt":
                creation_flags = (
                    subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW
                )
            with self.console_log.open("a", encoding="utf-8") as console:
                self.process = subprocess.Popen(
                    [
                        sys.executable,
                        str(ROOT / "main.py"),
                        "--config",
                        str(self.runtime_config),
                        "--control-file",
                        str(self.control_file),
                        "--run-id",
                        run_id,
                    ],
                    cwd=ROOT,
                    stdin=subprocess.DEVNULL,
                    stdout=console,
                    stderr=subprocess.STDOUT,
                    creationflags=creation_flags,
                )
            control["pid"] = self.process.pid
            control["process_state"] = "running"
            control["message"] = "机器人进程已启动，等待官方盘口"
            atomic_write_json(self.control_file, control)
            return self.status()

    async def stop(self) -> dict:
        async with self._lock:
            value = read_json(self.control_file, {})
            if not isinstance(value, dict) or not value.get("run_id"):
                return self.status()
            value["desired_state"] = "stop_requested"
            value["process_state"] = "stopping"
            value["message"] = "已请求安全撤单退出"
            value["dashboard_heartbeat_ms"] = int(time.time() * 1000)
            atomic_write_json(self.control_file, value)
            deadline = time.monotonic() + 8
            pid = int(value.get("pid") or 0)
            while _pid_exists(pid) and time.monotonic() < deadline:
                await asyncio.sleep(0.2)
            if not _pid_exists(pid):
                self._converge_requested_stop(str(value.get("run_id") or ""), pid)
            return self.status()

    def _heartbeat_once(self) -> None:
        """Reconcile observed process state without erasing the operator's intent.

        ``desired_state`` is an intent flag consumed by both the bot and the
        external recovery monitor.  An unexpected exit must therefore leave it
        as ``running``; otherwise the monitor cannot distinguish a crash from an
        explicit stop request and will never attempt a safe recovery.
        """
        value = read_json(self.control_file, {})
        if not isinstance(value, dict) or value.get("desired_state") not in {
            "running",
            "stop_requested",
        }:
            return

        now_ms = int(time.time() * 1000)
        pid = int(value.get("pid") or 0)
        desired_state = value.get("desired_state")
        if desired_state == "stop_requested":
            alive = _pid_exists(pid)
            if self.process is not None and self.process.pid == pid:
                exit_code = self.process.poll()
                if exit_code is not None:
                    alive = False
            if alive:
                value["dashboard_heartbeat_ms"] = now_ms
                value["process_state"] = "stopping"
                value["message"] = "已请求安全撤单退出"
                atomic_write_json(self.control_file, value)
            else:
                self._converge_requested_stop(
                    str(value.get("run_id") or ""), pid, now_ms=now_ms
                )
            return

        started_at_ms = int(value.get("started_at_ms") or 0)
        if (
            pid <= 0
            and value.get("process_state") == "starting"
            and started_at_ms > 0
            and now_ms - started_at_ms <= STARTUP_PID_GRACE_MS
        ):
            value["dashboard_heartbeat_ms"] = now_ms
            value.pop("unexpected_exit", None)
            value.pop("unexpected_exit_at_ms", None)
            atomic_write_json(self.control_file, value)
            return

        alive = _pid_exists(pid)
        exit_code = None
        if self.process is not None and self.process.pid == pid:
            exit_code = self.process.poll()
            if exit_code is not None:
                alive = False

        value["dashboard_heartbeat_ms"] = now_ms
        if alive:
            value["process_state"] = "running"
            if value.get("message") in {
                "正在启动",
                "机器人进程已启动，等待官方盘口",
                "机器人进程已登记，正在执行启动核验",
                "机器人进程已退出，等待诊断恢复",
            }:
                value["message"] = "机器人进程运行中"
            value.pop("unexpected_exit", None)
            value.pop("unexpected_exit_at_ms", None)
        else:
            value["process_state"] = "exited"
            value["unexpected_exit"] = True
            value.setdefault("unexpected_exit_at_ms", now_ms)
            value["message"] = "机器人进程已退出，等待诊断恢复"
            if exit_code is not None:
                value["last_exit_code"] = exit_code
        atomic_write_json(self.control_file, value)

    def _current_summary(self, strategy: str | None) -> dict:
        if strategy not in {"market_maker", "grid"}:
            return {}
        runtime = read_json(self.runtime_config, {})
        runtime_section = runtime.get("runtime") if isinstance(runtime, dict) else {}
        state_dir_value = (
            runtime_section.get("state_dir")
            if isinstance(runtime_section, dict)
            else None
        ) or "state"
        state_dir = Path(state_dir_value)
        if not state_dir.is_absolute():
            state_dir = ROOT / state_dir
        path = state_dir / f"live_{strategy}_io_{self.symbol}_summary.json"
        value = read_json(path, {})
        return value if isinstance(value, dict) else {}

    def _summary_for_control(self, control: dict) -> dict:
        """Return only a summary proven to belong to this controller run."""

        summary = self._current_summary(control.get("strategy"))
        if not summary:
            return {}
        expected_session = str(control.get("summary_session_id") or "")
        actual_session = str(summary.get("session_id") or "")
        if expected_session:
            if actual_session != expected_session:
                return {}
        else:
            # Legacy/current-startup fallback before the engine has published
            # its exact session binding. A prior run's summary is never enough
            # to prove a clean stop.
            started_at_ms = int(control.get("started_at_ms") or 0)
            summary_started_at_ms = int(summary.get("session_started_at_ms") or 0)
            if started_at_ms <= 0 or summary_started_at_ms < started_at_ms:
                return {}
        started_at_ms = int(control.get("started_at_ms") or 0)
        updated_at_ms = int(summary.get("updated_at_ms") or 0)
        if started_at_ms > 0 and updated_at_ms < started_at_ms:
            return {}
        return summary

    def _startup_failed_before_signed_mutation(self, control: dict) -> bool:
        """Recognize a dead child that failed in read-only startup checks."""

        if _pid_exists(int(control.get("pid") or 0)):
            return False
        started_at_ms = int(control.get("started_at_ms") or 0)
        safe_hints = (
            "最近 15 分钟",
            "开放委托中检测到",
            "wallet.local.json",
            "钱包私钥",
            "主钱包私钥",
            "HL_ACCOUNT_ADDRESS",
            "portfolioMargin",
            "签名钱包不是可用的主钱包或 API Wallet",
        )
        for line in reversed(read_log_tail(self.console_log, lines=120)):
            if "启动失败:" not in line or not any(hint in line for hint in safe_hints):
                continue
            try:
                line_ms = int(
                    datetime.strptime(line[:19], "%Y-%m-%d %H:%M:%S").timestamp()
                    * 1000
                )
            except (ValueError, OverflowError, OSError):
                continue
            if started_at_ms <= 0 or line_ms + 2_000 >= started_at_ms:
                return True
        return False

    async def _gate_previous_session_summary(self) -> None:
        """Converge a prior session from the user's normal Start action.

        A dead child can leave its last summary as ``running`` or ``paused``.
        The green Start button is authorization to run the bounded, idempotent
        ledger repair already used by unattended recovery.  Unknown ownership,
        external activity, open orders, or an over-limit repair still fail
        closed, but routine recovery never sends the user to a Python command.
        """

        control = read_json(self.control_file, {})
        if not isinstance(control, dict):
            return
        raw_summary = self._current_summary(control.get("strategy"))
        if not raw_summary:
            return
        if control.get("strategy") == "grid":
            # The stopped-fill repair tool addresses the market-maker ledger.
            # Grid restarts reconcile their durable inventory/fill journal in
            # the engine before quoting; never rewrite them through that tool.
            if raw_summary.get("open_order_details") or _number(raw_summary.get("open_orders")):
                raise web.HTTPConflict(text="上一网格会话仍有未完成委托，请先完成撤单对账；库存台账已保留")
            if raw_summary.get("grid_accounting", {}).get("status") == "error":
                raise web.HTTPConflict(text="上一网格库存台账需要对账，已阻止启动并保留真实库存")
            return
        summary = self._summary_for_control(control)
        if not summary:
            if self._startup_failed_before_signed_mutation(control):
                # The newest child stopped before any signer/exchange object
                # was created.  Reconcile the last durable session through the
                # same bounded official gate instead of trapping the green
                # button behind a stale run id.
                summary = raw_summary
            else:
                raise web.HTTPConflict(
                    text=(
                        "上一停机摘要无法确认属于当前运行；旧会话已保留，"
                        "面板未发送订单"
                    )
                )
        try:
            plan = await reconcile_stopped_market(
                self.symbol,
                wallet_id=self.wallet_id,
                apply=False,
                max_fills=SAFE_HARD_RECOVERY_MAX_FILLS,
            )
        except Exception as exc:
            raise web.HTTPConflict(
                text=(
                    "启动前停机对账不可用"
                    f"（{_safe_reconciliation_error_label(exc)}）。"
                    "请检查面板中的钱包配置；旧会话已保留，面板未发送订单"
                )
            ) from exc
        pre_apply_blockers = {
            "local_extra_count": int(plan.get("local_extra_count") or 0),
            "official_unknown_fill_count": int(
                plan.get("official_unknown_fill_count") or 0
            ),
            "official_unknown_order_count": int(
                plan.get("official_unknown_order_count") or 0
            ),
            "official_open_orders": int(plan.get("official_open_orders") or 0),
            "ioc_pending_count": int(plan.get("ioc_pending_count") or 0),
            "order_pending_count": int(plan.get("order_pending_count") or 0),
        }
        recoverable_count = int(plan.get("recoverable_count") or 0)
        if (
            recoverable_count
            and bool(plan.get("apply_eligible"))
            and not any(pre_apply_blockers.values())
        ):
            try:
                plan = await reconcile_stopped_market(
                    self.symbol,
                    wallet_id=self.wallet_id,
                    apply=True,
                    max_fills=SAFE_HARD_RECOVERY_MAX_FILLS,
                )
            except Exception as exc:
                raise web.HTTPConflict(
                    text=(
                        "一键启动自动补齐停机成交失败"
                        f"（{_safe_reconciliation_error_label(exc)}）；"
                        "旧会话已保留，面板未发送订单"
                    )
                ) from exc
        blockers = {
            "recoverable_count": int(plan.get("recoverable_count") or 0),
            "local_extra_count": int(plan.get("local_extra_count") or 0),
            "official_unknown_fill_count": int(
                plan.get("official_unknown_fill_count") or 0
            ),
            "official_unknown_order_count": int(
                plan.get("official_unknown_order_count") or 0
            ),
            "official_open_orders": int(plan.get("official_open_orders") or 0),
            "ioc_pending_count": int(plan.get("ioc_pending_count") or 0),
            "order_pending_count": int(plan.get("order_pending_count") or 0),
        }
        blocked = (
            not bool(plan.get("reconciliation_ok"))
            or any(blockers.values())
        )
        if blocked:
            detail = ", ".join(
                f"{key}={value}" for key, value in blockers.items() if value
            ) or str(plan.get("blocked_reason") or "reconciliation_not_clean")
            raise web.HTTPConflict(
                text=(
                    f"启动前停机对账未收敛（{detail}）；"
                    "旧会话已保留，面板未发送订单"
                )
            )

    def _converge_requested_stop(
        self, run_id: str, pid: int, *, now_ms: int | None = None
    ) -> None:
        """Converge an exited stop request from the current run's final summary."""

        value = read_json(self.control_file, {})
        if not (
            isinstance(value, dict)
            and run_id
            and value.get("run_id") == run_id
            and value.get("desired_state") == "stop_requested"
        ):
            return
        now_ms = now_ms or int(time.time() * 1000)
        summary = self._summary_for_control(value)
        summary_status = summary.get("status")
        open_orders = summary.get("open_orders")
        clean = summary_status == "stopped" and open_orders == 0
        if clean:
            value["desired_state"] = "stopped"
            value["process_state"] = "stopped"
            value["message"] = "已安全停止（官方挂单终态已确认）"
        else:
            reason = str(summary.get("reason") or "")
            if summary_status == "halted" and reason:
                message = reason
            elif summary_status == "stopped":
                message = "退出摘要未确认挂单已清空，需要核对官方挂单"
            else:
                message = "机器人已退出，但当前运行的最终摘要缺失或未确认干净"
            value["desired_state"] = "halted"
            value["process_state"] = "halted"
            value["message"] = f"安全停机未收敛：{message}"
        value["dashboard_heartbeat_ms"] = now_ms
        value.pop("unexpected_exit", None)
        value.pop("unexpected_exit_at_ms", None)
        if self.process is not None and self.process.pid == pid:
            exit_code = self.process.poll()
            if exit_code is not None:
                value["last_exit_code"] = exit_code
        atomic_write_json(self.control_file, value)

    def _recovery_block_reason(self, status: dict) -> str | None:
        summary = self._current_summary(status.get("strategy"))
        if summary.get("status") == "halted":
            return str(summary.get("reason") or "风险停机")
        started_at_ms = int(status.get("started_at_ms") or 0)
        current_lines: list[str] = []
        for line in read_log_tail(self.console_log, lines=80):
            try:
                line_ms = int(
                    datetime.strptime(line[:19], "%Y-%m-%d %H:%M:%S").timestamp()
                    * 1000
                )
            except (ValueError, OverflowError):
                line_ms = started_at_ms
            if not started_at_ms or line_ms + 2_000 >= started_at_ms:
                current_lines.append(line)
        tail = "\n".join(current_lines)
        return next((hint for hint in _HARD_RECOVERY_BLOCK_HINTS if hint in tail), None)

    def _block_auto_recovery(self, reason: str) -> None:
        value = read_json(self.control_file, {})
        if not isinstance(value, dict):
            return
        value["desired_state"] = "halted"
        value["process_state"] = "halted"
        value["message"] = f"自动恢复已阻止：{reason}"
        value["dashboard_heartbeat_ms"] = int(time.time() * 1000)
        atomic_write_json(self.control_file, value)

    async def auto_recover_once(self) -> bool:
        """Restart one crashed child only after proving it was not a risk halt."""

        if self._recovering:
            return False
        status = self.status()
        if (
            status.get("running")
            or status.get("desired_state") != "running"
            or status.get("process_state") != "exited"
        ):
            return False
        value = read_json(self.control_file, {})
        if not isinstance(value, dict):
            return False
        now_ms = int(time.time() * 1000)
        exited_at_ms = int(value.get("unexpected_exit_at_ms") or now_ms)
        if now_ms - exited_at_ms < AUTO_RECOVERY_DELAY_MS:
            return False
        block_reason = self._recovery_block_reason(status)
        if block_reason:
            self._block_auto_recovery(block_reason)
            return False
        attempts = self._load_recovery_attempts(now_ms)
        if len(attempts) >= AUTO_RECOVERY_MAX_ATTEMPTS:
            self._block_auto_recovery("一小时内瞬时恢复已达到3次上限")
            return False
        strategy = status.get("strategy")
        if strategy not in {"market_maker", "grid"}:
            self._block_auto_recovery("缺少可恢复的策略类型")
            return False
        attempts.append(now_ms)
        value["auto_recovery_attempts_ms"] = list(attempts)
        value["auto_recovery_count"] = len(attempts)
        atomic_write_json(self.control_file, value)
        self._recovering = True
        try:
            recovery_hold = (
                value.get("recovery_hold_until_session_pause") is True
            )
            if recovery_hold:
                await self.start(
                    strategy,
                    recovery_hold_until_session_pause=True,
                )
            else:
                await self.start(strategy)
            return True
        except Exception as exc:
            failed = read_json(self.control_file, {})
            if isinstance(failed, dict):
                failed["desired_state"] = "running"
                failed["process_state"] = "exited"
                failed["unexpected_exit"] = True
                failed["unexpected_exit_at_ms"] = now_ms
                failed["message"] = (
                    f"自动恢复启动失败：{type(exc).__name__}，等待退避后重试"
                )
                atomic_write_json(self.control_file, failed)
            return False
        finally:
            self._recovering = False

    @staticmethod
    def _is_safe_transient_halt_reason(reason: str) -> bool:
        return "连续主循环错误" in reason and any(
            hint in reason for hint in _SAFE_TRANSIENT_HALT_HINTS
        )

    def _record_safe_hard_recovery_outcome(
        self, run_id: str, outcome: str, message: str
    ) -> None:
        value = read_json(self.control_file, {})
        if not isinstance(value, dict) or value.get("run_id") != run_id:
            return
        value["safe_hard_recovery_checked_run_id"] = run_id
        value["safe_hard_recovery_outcome"] = outcome
        value["message"] = message
        value["dashboard_heartbeat_ms"] = int(time.time() * 1000)
        atomic_write_json(self.control_file, value)

    async def auto_recover_safe_halt_once(self) -> bool:
        """Recover a legacy network halt only through the strict local gate."""

        if self._hard_recovering:
            return False
        status = self.status()
        if (
            status.get("running")
            or status.get("desired_state") != "halted"
            or status.get("process_state") != "halted"
            or status.get("strategy") != "market_maker"
        ):
            return False
        value = read_json(self.control_file, {})
        if not isinstance(value, dict):
            return False
        run_id = str(value.get("run_id") or "")
        if not run_id or value.get("safe_hard_recovery_checked_run_id") == run_id:
            return False
        now_ms = int(time.time() * 1000)
        if now_ms < int(value.get("safe_hard_recovery_retry_after_ms") or 0):
            return False
        summary = self._current_summary("market_maker")
        reason = str(summary.get("reason") or "")
        if not self._is_safe_transient_halt_reason(reason):
            return False
        updated_at_ms = int(summary.get("updated_at_ms") or 0)
        if updated_at_ms and now_ms - updated_at_ms < AUTO_RECOVERY_DELAY_MS:
            return False
        # Shared rolling-hour circuit breaker with the unexpected-exit
        # recovery: a persistent fault must escalate to a human instead of
        # restart-looping against the exchange.
        attempts = self._load_recovery_attempts(now_ms)
        if len(attempts) >= AUTO_RECOVERY_MAX_ATTEMPTS:
            self._record_safe_hard_recovery_outcome(
                run_id,
                "blocked_attempt_limit",
                "安全恢复已阻止：一小时内自动恢复已达 3 次上限，需人工介入",
            )
            return False

        self._hard_recovering = True
        try:
            plan = await reconcile_stopped_market(
                self.symbol,
                wallet_id=self.wallet_id,
                apply=False,
                max_fills=SAFE_HARD_RECOVERY_MAX_FILLS,
            )
            if plan.get("external_activity_detected"):
                self._record_safe_hard_recovery_outcome(
                    run_id,
                    "blocked_external_activity",
                    "安全恢复已阻止：检测到同市场外部成交活动",
                )
                return False
            if not plan.get("apply_eligible"):
                count = int(plan.get("recoverable_count") or 0)
                self._record_safe_hard_recovery_outcome(
                    run_id,
                    str(plan.get("blocked_reason") or "blocked"),
                    f"安全恢复已阻止：缺失成交 {count} 笔未通过10笔补录门槛",
                )
                return False
            if int(plan.get("recoverable_count") or 0):
                plan = await reconcile_stopped_market(
                    self.symbol,
                    wallet_id=self.wallet_id,
                    apply=True,
                    max_fills=SAFE_HARD_RECOVERY_MAX_FILLS,
                )
            if not plan.get("reconciliation_ok"):
                self._record_safe_hard_recovery_outcome(
                    run_id,
                    "blocked_reconciliation",
                    "安全恢复已阻止：官方成交与本地账本仍未一致",
                )
                return False
            # A non-zero official position deliberately does NOT block
            # recovery: the engine's startup reconciliation adopts the
            # clearinghouse position as the authority, and the loss lines
            # plus flatten_on_halt bound any residual risk.  Leaving
            # inventory unmanaged while halted is the greater danger.
            attempts.append(now_ms)
            # A 7x24 market (no US-session pause) would never leave the
            # monitor-only recovery hold, so it restarts normally; gated
            # markets keep the conservative hold-until-session-pause.
            hold = bool(
                (self.runtime_overrides.get("runtime") or {}).get(
                    "pause_during_us_regular_session", True
                )
            )
            await self.start(
                "market_maker",
                recovery_hold_until_session_pause=hold,
            )
            return True
        except Exception as exc:
            latest = read_json(self.control_file, {})
            if isinstance(latest, dict) and latest.get("run_id") == run_id:
                latest["safe_hard_recovery_retry_after_ms"] = (
                    int(time.time() * 1000) + SAFE_HARD_RECOVERY_RETRY_MS
                )
                latest["message"] = (
                    f"安全恢复核验暂不可用：{type(exc).__name__}，稍后重试"
                )
                atomic_write_json(self.control_file, latest)
            return False
        finally:
            self._hard_recovering = False

    async def heartbeat(self) -> None:
        while True:
            await asyncio.sleep(2)
            self._heartbeat_once()


def load_dashboard_settings() -> dict:
    """Use explicit local settings or the shipped small-budget public defaults."""
    path = DASHBOARD_SETTINGS_FILE
    if not path.exists():
        path = path.with_name("dashboard.config.example.json")
    if not path.is_file():
        raise RuntimeError(
            "面板预算配置缺失：需要 dashboard.config.json 或 dashboard.config.example.json；拒绝使用旧实盘参数"
        )
    return json.loads(path.read_text(encoding="utf-8"))


class PortfolioController:
    """Run one isolated bot process per market under one dashboard intent."""

    def __init__(self, monitors: dict[str, LiveMonitor], *, settings: dict | None = None) -> None:
        self.controllers: dict[str, BotController] = {}
        self._lock = asyncio.Lock()
        explicit_wallets = sorted(
            {key.split(":", 1)[0] for key in monitors if ":" in key}
        )
        self.wallet_ids = explicit_wallets or ["primary"]
        settings = load_dashboard_settings() if settings is None else settings
        base_overrides = {
            "risk": {
                "capital_usd": 6000.0,
                # Baseline values are overridden by the explicit dashboard settings.
                # Keep per-side sizing and the rebalance threshold consistent.
                "max_position_usd": 3600.0,
                "max_open_order_usd": 4200.0,
                "min_order_notional_usd": 10.0,
                "min_free_collateral_usd": 600.0,
                # Risk thresholds separate short-window, session and principal loss.
                # Explicit dashboard settings determine the limits used at startup.
                "max_session_drawdown_usd": 200.0,
                "max_principal_drawdown_usd": 360.0,
                "max_rolling_drawdown_usd": 60.0,
                "rolling_drawdown_window_min": 30.0,
                "daily_loss_budget_usd": 0.0,
                # Unattended operation: a risk halt must also close the
                # residual inventory (reduce-only IOC capped by
                # emergency_slippage_bps), otherwise the drawdown line only
                # stops quoting while the open position keeps bleeding until
                # a human shows up.  This is what makes total loss bounded.
                "flatten_on_halt": True,
                "auto_scale_with_equity": True,
                "auto_scale_max_equity_usd": 6000.0,
                "auto_scale_step_usd": 100.0,
                "order_notional_pct_of_equity": 30.0,
                "order_notional_cap_usd": 1800.0,
                "max_position_pct_of_equity": 60.0,
                "max_open_order_pct_of_equity": 70.0,
                "min_free_collateral_pct_of_equity": 10.0,
                "max_session_drawdown_pct_of_equity": 3.4,
                "max_principal_drawdown_pct_of_equity": 6.0,
            },
            "market_maker": {
                "levels": 1,
                # Total budget for one side of the ladder (not per level).
                "order_notional_usd": 1800.0,
                "half_spread_bps": 1.2,
                "level_spacing_bps": 3.0,
                "level_notional_decay": 0.35,
                "microprice_weight": 0.0,
                "vol_spread_mult": 0.0,
                "aggressive_requote_multiplier": 3.0,
                "aggressive_requote_min_age_sec": 3.0,
                "qty_refresh_ratio": 0.05,
                "adaptive_spread_idle_decay_sec": 1800.0,
                "adaptive_spread_sample_max_age_sec": 21600.0,
            },
            "grid": {"order_notional_usd": 20.0},
        }
        for wallet_id in self.wallet_ids:
            wallet_slug = _safe_slug(wallet_id)
            for symbol in LIVE_MARKETS:
                runtime_config, control_file, console_log = market_runtime_paths(
                    symbol, wallet_id
                )
                overrides = json.loads(json.dumps(base_overrides))
                state_dir = (
                    "state"
                    if wallet_id == "primary"
                    else f"state/wallets/{wallet_slug}"
                )
                log_dir = (
                    f"logs/{symbol.lower()}"
                    if wallet_id == "primary"
                    else f"logs/wallets/{wallet_slug}/{symbol.lower()}"
                )
                overrides["runtime"] = {
                    "state_dir": state_dir,
                    "log_dir": log_dir,
                    # Pause this market around the regular cash session according to
                    # the configured schedule and buffers.
                    "pause_during_us_regular_session": True,
                    "us_regular_session_pre_open_buffer_min": 30,
                    "us_regular_session_post_close_buffer_min": 5,
                    # Websocket push-state + batchModify keep REST weight low;
                    # 5s is only the REST fallback cadence when the socket is
                    # degraded.  The 60s sweep stays authoritative.
                    "fill_refresh_sec": 5.0,
                    "rest_sweep_sec": 60.0,
                    "use_ws_user_feeds": True,
                    "use_batch_modify": True,
                    # Growth-mode volume only credits 10% toward the address
                    # action budget, so soft/hard throttling can bind before IP
                    # limits do.  Automatic purchases are intentionally off:
                    # poor $/action must be corrected rather than subsidized.
                    "budget_poll_sec": 60.0,
                    "budget_soft_remaining": 5000,
                    "budget_hard_remaining": 1500,
                    "auto_reserve_actions": False,
                    "reserve_actions_chunk": 10000,
                    "reserve_actions_daily_max_usd": 0.0,
                }
                overrides["live"] = {"wallet_id": wallet_id}
                overrides["market"] = {"symbol": symbol, "target_leverage": 3}
                if symbol == "ANTH":
                    # ANTH is a pre-IPO perp rather than an already-listed US
                    # cash equity; keep it eligible through RTH while its own
                    # volatility/markout guards remain authoritative.
                    overrides["runtime"]["pause_during_us_regular_session"] = False
                    overrides["market_maker"].update(
                        {
                            # ANTH carries heavier adverse selection: wider
                            # touch and wider ladder spacing.
                            "half_spread_bps": 2.5,
                            "level_spacing_bps": 4.0,
                        }
                    )
                    overrides["grid"]["order_notional_usd"] = 15.0
                key = symbol if len(self.wallet_ids) == 1 else f"{wallet_id}:{symbol}"
                if not isinstance(settings, dict) or set(settings) - {"defaults", "markets"}:
                    raise ValueError("dashboard.config.json 只允许 defaults 与 markets")
                defaults = settings.get("defaults", {})
                markets = settings.get("markets", {})
                if not isinstance(defaults, dict) or not isinstance(markets, dict):
                    raise ValueError("面板配置 defaults/markets 必须为对象")
                market_settings = markets.get(symbol, {})
                allowed = {"risk", "market_maker", "grid", "fees", "market", "runtime"}
                for overlay in (defaults, market_settings):
                    if not isinstance(overlay, dict) or set(overlay) - allowed:
                        raise ValueError("面板策略配置包含不支持的字段")
                    for section, forbidden in {
                        "market": {"symbol", "dex", "api_url", "ws_url"},
                        "runtime": {"state_dir", "log_dir"},
                    }.items():
                        part = overlay.get(section, {})
                        if not isinstance(part, dict) or set(part) & forbidden:
                            raise ValueError("面板预算配置不能覆盖市场身份、接口或钱包隔离路径")
                    _merge_dict(overrides, overlay)
                monitor_key = (
                    f"{wallet_id}:{symbol}" if explicit_wallets else symbol
                )
                self.controllers[key] = BotController(
                    monitors[monitor_key],
                    symbol=symbol,
                    wallet_id=wallet_id,
                    runtime_config=runtime_config,
                    control_file=control_file,
                    console_log=console_log,
                    runtime_overrides=overrides,
                )

    def market_status(self, wallet_id: str, symbol: str) -> dict:
        key = symbol if len(self.wallet_ids) == 1 else f"{wallet_id}:{symbol}"
        controller = self.controllers.get(key)
        return controller.status() if controller else {}

    def console_log_for(self, wallet_id: str, symbol: str) -> Path:
        key = symbol if len(self.wallet_ids) == 1 else f"{wallet_id}:{symbol}"
        controller = self.controllers.get(key)
        return controller.console_log if controller else market_runtime_paths(
            symbol, wallet_id
        )[2]

    def status(self) -> dict:
        children = {symbol: bot.status() for symbol, bot in self.controllers.items()}
        running_count = sum(1 for item in children.values() if item["running"])
        desired_count = sum(
            1 for item in children.values() if item["desired_state"] == "running"
        )
        strategies = {
            item.get("strategy") for item in children.values() if item.get("strategy")
        }
        strategy = next(iter(strategies)) if len(strategies) == 1 else None
        profiles = [
            item.get("risk_profile")
            for item in children.values()
            if isinstance(item.get("risk_profile"), dict)
        ]
        risk_profile = {}
        if profiles:
            risk_profile = {
                "configured_capital_usd": max(
                    _number(item.get("configured_capital_usd")) for item in profiles
                ),
                "order_notional_usd": sum(
                    _number(item.get("order_notional_usd")) for item in profiles
                ),
                "max_position_usd": sum(
                    _number(item.get("max_position_usd")) for item in profiles
                ),
                "max_open_order_usd": sum(
                    _number(item.get("max_open_order_usd")) for item in profiles
                ),
                "max_drawdown_usd": min(
                    _number(item.get("max_drawdown_usd")) for item in profiles
                ),
                "max_principal_drawdown_usd": min(
                    _number(item.get("max_principal_drawdown_usd"))
                    for item in profiles
                ),
            }
        target_label = (
            "双市场" if len(self.wallet_ids) == 1
            else f"{len(self.wallet_ids)} 钱包 × {len(LIVE_MARKETS)} 市场"
        )
        if running_count == len(children):
            message = f"{target_label}进程运行中（{running_count}/{len(children)}）"
            process_state = "running"
        elif running_count:
            message = f"仅有 {running_count}/{len(children)} 个市场进程运行，需检查"
            process_state = "partial"
        elif desired_count:
            message = "双市场进程已退出，等待诊断恢复"
            process_state = "exited"
        else:
            message = "双市场机器人已停止"
            process_state = "stopped"
        return {
            "running": running_count > 0,
            "all_running": running_count == len(children),
            "desired_state": "running" if desired_count else "stopped",
            "process_state": process_state,
            "strategy": strategy,
            "pid": None,
            "message": message,
            "risk_profile": risk_profile,
            "markets": children,
            "risk_profile_source": "planned" if all(item.get("risk_profile_source") == "planned" for item in children.values()) else "last_run",
            "wallet_count": len(self.wallet_ids),
        }

    def _write_aggregate_control(self) -> None:
        status = self.status()
        atomic_write_json(
            CONTROL_FILE,
            {
                "portfolio": True,
                "desired_state": status["desired_state"],
                "process_state": status["process_state"],
                "strategy": status["strategy"],
                "dashboard_heartbeat_ms": int(time.time() * 1000),
                "message": status["message"],
                "risk_profile": status["risk_profile"],
                "markets": status["markets"],
            },
        )

    async def start(
        self,
        strategy: str,
        *,
        recovery_hold_until_session_pause: bool = False,
    ) -> dict:
        if strategy not in {"market_maker", "grid"}:
            raise web.HTTPBadRequest(text="策略只能是 market_maker 或 grid")
        async with self._lock:
            if any(item.status()["running"] for item in self.controllers.values()):
                raise web.HTTPConflict(text="已有市场机器人进程在运行")
            started: list[BotController] = []
            try:
                for bot in self.controllers.values():
                    await bot.start(
                        strategy,
                        recovery_hold_until_session_pause=(
                            recovery_hold_until_session_pause
                        ),
                    )
                    started.append(bot)
            except Exception:
                for bot in started:
                    await bot.stop()
                self._write_aggregate_control()
                raise
            self._write_aggregate_control()
            return self.status()

    def _market_controller(self, wallet_id: str, symbol: str) -> BotController:
        wallet_id = str(wallet_id or "primary")
        symbol = str(symbol or "").upper()
        if symbol not in LIVE_MARKETS:
            raise web.HTTPBadRequest(text="未知市场")
        if wallet_id not in self.wallet_ids:
            raise web.HTTPBadRequest(text="未知钱包")
        key = symbol if len(self.wallet_ids) == 1 else f"{wallet_id}:{symbol}"
        controller = self.controllers.get(key)
        if controller is None:
            raise web.HTTPBadRequest(text="市场控制器不存在")
        return controller

    async def start_market(
        self,
        wallet_id: str,
        symbol: str,
        strategy: str,
        *,
        recovery_hold_until_session_pause: bool = False,
    ) -> dict:
        """Start one reconciled market without coupling it to halted peers."""

        if strategy not in {"market_maker", "grid"}:
            raise web.HTTPBadRequest(text="策略只能是 market_maker 或 grid")
        async with self._lock:
            bot = self._market_controller(wallet_id, symbol)
            if bot.status()["running"]:
                raise web.HTTPConflict(text="该市场机器人进程已在运行")
            try:
                await bot.start(
                    strategy,
                    recovery_hold_until_session_pause=(
                        recovery_hold_until_session_pause
                    ),
                )
            finally:
                self._write_aggregate_control()
            return self.status()

    async def stop_market(self, wallet_id: str, symbol: str) -> dict:
        """Safely stop one market while leaving independent peers untouched."""

        async with self._lock:
            bot = self._market_controller(wallet_id, symbol)
            try:
                await bot.stop()
            finally:
                self._write_aggregate_control()
            return self.status()

    async def stop(self) -> dict:
        async with self._lock:
            await asyncio.gather(
                *(bot.stop() for bot in self.controllers.values()),
                return_exceptions=False,
            )
            self._write_aggregate_control()
            return self.status()

    async def heartbeat(self) -> None:
        """Liveness heartbeat only. MUST never block.

        Live bots exit fail-safe when their dashboard heartbeat becomes stale.
        Recovery can require lengthy official reconciliation, so it runs in
        recovery_loop() without delaying this heartbeat.
        """
        while True:
            await asyncio.sleep(2)
            for bot in self.controllers.values():
                bot._heartbeat_once()
            self._write_aggregate_control()

    async def recovery_loop(self) -> None:
        while True:
            await asyncio.sleep(5)
            for bot in self.controllers.values():
                try:
                    await bot.auto_recover_once()
                except Exception as exc:
                    print(f"自动恢复检查异常 ({bot.symbol}): {exc!r}")
            for bot in self.controllers.values():
                try:
                    await bot.auto_recover_safe_halt_once()
                except Exception as exc:
                    print(f"安全恢复检查异常 ({bot.symbol}): {exc!r}")


def read_log_tail(path: Path, lines: int = 40) -> list[str]:
    if lines <= 0:
        return []
    try:
        with path.open("rb") as handle:
            handle.seek(0, os.SEEK_END)
            position, chunks, newlines = handle.tell(), [], 0
            while position > 0 and newlines <= lines:
                length = min(position, 8192)
                position -= length
                handle.seek(position)
                chunk = handle.read(length)
                chunks.append(chunk)
                newlines += chunk.count(b"\n")
        text = b"".join(reversed(chunks)).decode("utf-8", errors="replace")
    except OSError:
        return []
    return text.splitlines()[-lines:]


async def index(request: web.Request) -> web.Response:
    html = INDEX_FILE.read_text(encoding="utf-8")
    html = html.replace("__CONTROL_TOKEN__", request.app["control_token"])
    return web.Response(
        text=html,
        content_type="text/html",
        headers={
            "Cache-Control": "no-store",
            "X-Content-Type-Options": "nosniff",
            "X-Frame-Options": "DENY",
            "Content-Security-Policy": (
                "default-src 'self'; style-src 'self' 'unsafe-inline'; "
                "script-src 'self' 'unsafe-inline'; connect-src 'self'; "
                "img-src 'self' data:; frame-ancestors 'none'"
            ),
        },
    )


async def api_snapshot(request: web.Request) -> web.Response:
    local = build_local_snapshot(request.query.get("profile"))
    summary = local.get("summary") or {}
    coin = str(summary.get("coin") or "io:SNDK")
    symbol = coin.split(":")[-1].upper()
    wallet_id = str(summary.get("wallet_id") or "primary")
    monitors: dict[str, LiveMonitor] = request.app["monitors"]
    monitor_key = f"{wallet_id}:{symbol}"
    monitor = monitors.get(monitor_key) or next(iter(monitors.values()))
    portfolio: PortfolioController = request.app["controller"]
    controller_status = portfolio.status()
    market_control = portfolio.market_status(wallet_id, symbol)
    if controller_status.get("running"):
        # Never let a browser refresh compete with a live trader, even when
        # the selected profile is a stale pre-migration alias. Show local
        # telemetry age/staleness instead of falling back to official REST.
        live = live_from_bot_summary(summary)
    else:
        live = await monitor.snapshot()
    console_log = portfolio.console_log_for(wallet_id, monitor.symbol)
    value = {
        "ok": True,
        "read_only": bool(request.app.get("read_only")),
        **local,
        "live": live,
        "controller": controller_status,
        "bot_log": read_log_tail(console_log),
    }
    return web.json_response(value, headers={"Cache-Control": "no-store"})


def _require_control_token(request: web.Request) -> None:
    if request.method != "GET" and request.app.get("read_only"):
        raise web.HTTPForbidden(text="只读预览不允许保存配置或交易控制")
    if not secrets.compare_digest(
        request.headers.get("X-Entropy-Control", ""),
        request.app["control_token"],
    ):
        raise web.HTTPForbidden(text="控制令牌无效")


async def api_setup(request: web.Request) -> web.Response:
    _require_control_token(request)
    cfg = load_config(LIVE_TEMPLATE)
    controller = request.app["controller"]
    if request.method == "POST":
        # Read the potentially slow request body before testing recovery state.
        payload = await request.json()
        async with controller._lock:
            if controller.status().get("running") or any(
                bot._recovering or bot._hard_recovering or bot.status().get("desired_state") == "running"
                for bot in controller.controllers.values()
            ):
                raise web.HTTPConflict(text="请先安全停止全部策略，再修改钱包配置")
            try:
                save_wallet(cfg.live.credentials_file, payload)
            except (ValueError, RuntimeError, json.JSONDecodeError) as exc:
                # Helpers only raise fixed labels, never include submitted secrets.
                raise web.HTTPBadRequest(text=str(exc)) from None
            wallet_ids = configured_wallet_ids(cfg) or ["primary"]
            monitors = {f"{wallet_id}:{symbol}": LiveMonitor(request.app["session"], symbol, wallet_id)
                        for wallet_id in wallet_ids for symbol in LIVE_MARKETS}
            refreshed = PortfolioController(monitors)
            controller.controllers = refreshed.controllers
            controller.wallet_ids = refreshed.wallet_ids
            request.app["monitors"].clear()
            request.app["monitors"].update(monitors)
    return web.json_response({"ok": True, "wallets": wallet_status(cfg.live.credentials_file),
                              "running": bool(controller.status().get("running")),
                              "credential_note": (
                                  "primary 的进程环境变量或显式 .env 优先于本机钱包文件；请使用命名钱包隔离不同账户。"
                                  if os.environ.get(cfg.live.private_key_env) or os.environ.get(cfg.live.account_address_env)
                                  or (cfg.live.env_file and cfg.live.env_file.exists()) else ""
                              ),
                              "read_only": bool(request.app.get("read_only"))},
                             headers={"Cache-Control": "no-store"})


def _budget_edit_reason(controller: PortfolioController, *, read_only: bool = False) -> str:
    if read_only:
        return "只读预览不能保存预算"
    if controller.status().get("running") or any(
        bot._recovering or bot._hard_recovering or bot.status().get("desired_state") != "stopped"
        for bot in controller.controllers.values()
    ):
        return "请先安全停止全部策略及自动恢复，再保存固定预算"
    return ""


def _next_budget_payload(bot: BotController, template: dict) -> dict:
    payload = json.loads(json.dumps(template))
    payload["strategy"] = "market_maker"
    _merge_dict(payload, bot.runtime_overrides)
    # Grid has a separate per-wallet settings overlay at actual start. Budget
    # validation must include it even though this editor changes only MM size.
    payload.setdefault("grid", {}).update(bot._validate_grid_settings({}))
    apply_tuning_overrides(payload, bot.symbol)
    return payload


def budget_state(app: web.Application, *, saved_symbol: str | None = None) -> dict:
    controller: PortfolioController = app["controller"]
    read_only = bool(app.get("read_only"))
    reason = _budget_edit_reason(controller, read_only=read_only)
    base = load_config(LIVE_TEMPLATE)
    template = json.loads(LIVE_TEMPLATE.read_text(encoding="utf-8"))
    result = {}
    for symbol in LIVE_MARKETS:
        bots = [bot for bot in controller.controllers.values() if bot.symbol == symbol]
        if not bots:
            continue
        payload = _next_budget_payload(bots[0], template)
        cfg, effective = budget_preview(base, payload)
        tuning = payload.get("_tuning_applied") or {}
        market_reason = reason or (
            "调参覆盖与预算字段冲突，请先移除该市场对应的调参字段"
            if tuning_budget_conflict(tuning) else ""
        )
        result[symbol] = {
            "values": values_from_config(cfg),
            "editable": not market_reason,
            "reason": market_reason,
            "wallet_ids": [bot.wallet_id for bot in bots],
            "auto_scale_with_equity": cfg.risk.auto_scale_with_equity,
            "limits": {"min_order_notional_usd": cfg.risk.min_order_notional_usd,
                       "capital_usd": cfg.risk.capital_usd,
                       "crisis_quote_notional_usd": cfg.market_maker.crisis_quote_notional_usd,
                       "grid_order_notional_usd": max(
                           bot._validate_grid_settings({})["order_notional_usd"] for bot in bots)},
            "effective": effective,
            "tuning_applied": tuning,
            "tuning_rejected": payload.get("_tuning_rejected") or {},
            "next_runtime_overrides": {section: payload[section]
                                       for section in ("risk", "market_maker", "grid", "fees")},
        }
    return {"ok": True, "read_only": read_only,
            "running": bool(controller.status().get("running")),
            "scope": "all_enabled_wallets_for_market", "saved_symbol": saved_symbol,
            "markets": result}


async def api_budget(request: web.Request) -> web.Response:
    _require_control_token(request)
    saved_symbol = None
    try:
        if request.method == "POST":
            # A slow body cannot hold a stale stopped-state decision.
            try:
                payload = await request.json()
            except (ValueError, TypeError):
                raise BudgetValidationError("预算请求不是有效 JSON") from None
            symbol, values = parse_budget_request(payload, LIVE_MARKETS)
            controller: PortfolioController = request.app["controller"]
            async with controller._lock:
                reason = _budget_edit_reason(controller)
                if reason:
                    return web.json_response({"ok": False, "error": reason}, status=409)
                tuning, _ = load_tuning_overrides(symbol)
                if tuning_budget_conflict(tuning):
                    return web.json_response({"ok": False, "error":
                        "调参覆盖与预算字段冲突，预算未保存；请先移除该市场对应的调参字段"}, status=409)
                candidate = with_fixed_budget(load_dashboard_settings(), symbol, values)
                prepared = PortfolioController(request.app["monitors"], settings=candidate)
                template = json.loads(LIVE_TEMPLATE.read_text(encoding="utf-8"))
                base = load_config(LIVE_TEMPLATE)
                updates = {}
                for key, bot in prepared.controllers.items():
                    if bot.symbol != symbol:
                        continue
                    next_payload = _next_budget_payload(bot, template)
                    cfg, _ = budget_preview(base, next_payload)
                    if tuning_budget_conflict(next_payload.get("_tuning_applied") or {}):
                        return web.json_response({"ok": False, "error":
                            "调参覆盖与预算字段冲突，预算未保存；请先移除该市场对应的调参字段"}, status=409)
                    if values_from_config(cfg) != values or cfg.risk.auto_scale_with_equity:
                        raise BudgetValidationError("实际生效预算与输入不一致，预算未保存")
                    validate_budget_relations(values, cfg)
                    validate_complete_payload(next_payload)
                    updates[key] = bot.runtime_overrides
                if not updates or any(key not in controller.controllers for key in updates):
                    raise BudgetValidationError("市场控制器已变化，请刷新页面后重试")
                # Prepare/serialize the response before the commit as well, so
                # a bad untouched profile cannot turn a successful save into an
                # apparent validation failure after the file already changed.
                prepared.controllers = {key: (prepared.controllers[key] if key in updates else bot)
                                        for key, bot in controller.controllers.items()}
                staged_app = dict(request.app)
                staged_app["controller"] = prepared
                response = web.json_response(budget_state(staged_app, saved_symbol=symbol),
                                             headers={"Cache-Control": "no-store"})
                # No await from the final stopped-state check through fsync and
                # replacement: recovery cannot interleave a start with this save.
                atomic_write_json(DASHBOARD_SETTINGS_FILE, candidate)
                for key, overrides in updates.items():
                    controller.controllers[key].runtime_overrides = overrides
                return response
        value = budget_state(request.app, saved_symbol=saved_symbol)
        return web.json_response(value, headers={"Cache-Control": "no-store"})
    except BudgetValidationError as exc:
        return web.json_response({"ok": False, "error": str(exc)}, status=400)
    except (ValueError, TypeError, KeyError, RuntimeError):
        return web.json_response({"ok": False, "error": "预算配置无法解析或校验，请检查配置文件后重试"}, status=400)
    except OSError:
        return web.json_response({"ok": False, "error": "预算文件读写失败，原配置已保留；请检查文件权限后重试"}, status=500)


@web.middleware
async def local_origin_only(request: web.Request, handler):
    # A remote page must not acquire the control token through DNS rebinding.
    host = request.host.rsplit(":", 1)[0] if ":" in request.host else request.host
    if host.lower() not in {"localhost", "127.0.0.1"}:
        raise web.HTTPForbidden(text="仅接受本机地址")
    origin = request.headers.get("Origin")
    if origin and origin not in {f"http://{request.host}", f"https://{request.host}"}:
        raise web.HTTPForbidden(text="禁止跨站控制请求")
    return await handler(request)


async def api_start(request: web.Request) -> web.Response:
    _require_control_token(request)
    payload = await request.json()
    strategy = str(payload.get("strategy") or "")
    value = await request.app["controller"].start(strategy)
    return web.json_response({"ok": True, "controller": value})


async def api_recovery_hold(request: web.Request) -> web.Response:
    """Start live monitors now while deferring all quotes to a safe window."""

    _require_control_token(request)
    payload = await request.json()
    strategy = str(payload.get("strategy") or "")
    value = await request.app["controller"].start(
        strategy,
        recovery_hold_until_session_pause=True,
    )
    return web.json_response({"ok": True, "controller": value})


async def api_market_start(request: web.Request) -> web.Response:
    _require_control_token(request)
    payload = await request.json()
    value = await request.app["controller"].start_market(
        str(payload.get("wallet_id") or "primary"),
        str(payload.get("symbol") or ""),
        str(payload.get("strategy") or ""),
    )
    return web.json_response({"ok": True, "controller": value})


async def api_market_recovery_hold(request: web.Request) -> web.Response:
    """Start one reconciled market in persisted monitor-only isolation."""

    _require_control_token(request)
    payload = await request.json()
    value = await request.app["controller"].start_market(
        str(payload.get("wallet_id") or "primary"),
        str(payload.get("symbol") or ""),
        str(payload.get("strategy") or ""),
        recovery_hold_until_session_pause=True,
    )
    return web.json_response({"ok": True, "controller": value})


async def api_market_stop(request: web.Request) -> web.Response:
    _require_control_token(request)
    payload = await request.json()
    value = await request.app["controller"].stop_market(
        str(payload.get("wallet_id") or "primary"),
        str(payload.get("symbol") or ""),
    )
    return web.json_response({"ok": True, "controller": value})


async def api_grid_settings(request: web.Request) -> web.Response:
    _require_control_token(request)
    payload = await request.json()
    portfolio = request.app["controller"]
    async with portfolio._lock:
        bot = portfolio._market_controller(
            str(payload.get("wallet_id") or "primary"),
            str(payload.get("symbol") or ""),
        )
        settings = await bot.save_grid_settings(payload.get("grid"))
    return web.json_response({"ok": True, "grid_settings": settings})


async def api_stop(request: web.Request) -> web.Response:
    _require_control_token(request)
    value = await request.app["controller"].stop()
    return web.json_response({"ok": True, "controller": value})


async def app_startup(app: web.Application) -> None:
    session = build_rest_session()
    base = load_config(LIVE_TEMPLATE)
    wallet_ids = configured_wallet_ids(base) or ["primary"]
    monitors = {
        f"{wallet_id}:{symbol}": LiveMonitor(session, symbol, wallet_id)
        for wallet_id in wallet_ids
        for symbol in LIVE_MARKETS
    }
    controller = PortfolioController(monitors)
    app["session"] = session
    app["monitors"] = monitors
    app["controller"] = controller
    if app.get("read_only"):
        return
    app["heartbeat_task"] = asyncio.create_task(
        controller.heartbeat(), name="entropy-ui-heartbeat"
    )
    app["recovery_task"] = asyncio.create_task(
        controller.recovery_loop(), name="entropy-ui-recovery"
    )


async def app_cleanup(app: web.Application) -> None:
    controller: PortfolioController = app["controller"]
    if not app.get("read_only") and controller.status().get("running"):
        await controller.stop()
    tasks = [app[key] for key in ("heartbeat_task", "recovery_task") if key in app]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await app["session"].close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Entropy 本地交易控制与统计面板")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--no-open", action="store_true")
    parser.add_argument("--read-only", action="store_true", help="只读预览：禁止配置、交易、心跳与自动恢复写入")
    args = parser.parse_args()
    url = f"http://127.0.0.1:{args.port}"
    if _existing_entropy_dashboard(args.port, required_read_only=args.read_only):
        print(f"Entropy 本地控制面板已在运行: {url}")
        if not args.no_open:
            webbrowser.open(url)
        return
    app = web.Application(client_max_size=16 * 1024, middlewares=[local_origin_only])
    app["control_token"] = secrets.token_urlsafe(32)
    app["read_only"] = args.read_only
    app.on_startup.append(app_startup)
    app.on_cleanup.append(app_cleanup)
    app.router.add_get("/", index)
    app.router.add_get("/api/snapshot", api_snapshot)
    app.router.add_get("/api/setup", api_setup)
    app.router.add_post("/api/setup", api_setup)
    app.router.add_get("/api/budget", api_budget)
    app.router.add_post("/api/budget", api_budget)
    app.router.add_post("/api/control/start", api_start)
    app.router.add_post("/api/control/recovery-hold", api_recovery_hold)
    app.router.add_post("/api/control/market/start", api_market_start)
    app.router.add_post(
        "/api/control/market/recovery-hold", api_market_recovery_hold
    )
    app.router.add_post("/api/control/market/stop", api_market_stop)
    app.router.add_post("/api/control/grid/settings", api_grid_settings)
    app.router.add_post("/api/control/stop", api_stop)
    if not args.no_open:
        threading.Timer(0.8, lambda: webbrowser.open(url)).start()
    print(f"Entropy 本地控制面板: {url}")
    print("仅监听本机 127.0.0.1；只读预览不执行交易控制或配置写入。" if args.read_only else
          "仅监听本机 127.0.0.1；关闭面板时会请求机器人安全撤单退出。")
    web.run_app(app, host="127.0.0.1", port=args.port, print=None)


if __name__ == "__main__":
    main()
