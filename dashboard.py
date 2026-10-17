from __future__ import annotations

import argparse
import asyncio
import ctypes
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
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, Request, build_opener

import aiohttp
from aiohttp import web

from entropy_bot.broker import order_ownership
from entropy_bot.config import load_config
from entropy_bot.credentials import configured_wallet_ids, resolve_live_credentials, wallet_storage_slug
from entropy_bot.credential_store import CredentialStorageError
from entropy_bot.fees import build_fee_model
from entropy_bot.market import MarketClient, build_rest_session
from entropy_bot.storage import atomic_write_json
from dashboard_recovery import RecoveryControllerMixin, RecoveryDependencies
from dashboard_http import DashboardHTTP, DashboardServices
from dashboard_monitor import LiveMonitor as AccountMonitor, MonitorServices
from dashboard_snapshot import (
    SnapshotRepository, SnapshotServices,
    _number as _number, calculate_exposure as calculate_exposure,
    overlay_fresh_bot_telemetry as overlay_fresh_bot_telemetry,
    live_from_bot_summary as live_from_bot_summary,
)
from dashboard_setup import save_wallet, wallet_status
from dashboard_charts import build_charts


ROOT = Path(__file__).resolve().parent
STATE_DIR = ROOT / "state"
LOG_DIR = ROOT / "logs"
INDEX_FILE = ROOT / "dashboard" / "index.html"
LIVE_TEMPLATE = ROOT / "config.live.example.json"
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


def _snapshot_services() -> SnapshotServices:
    return SnapshotServices(STATE_DIR, read_json, build_charts)


_SNAPSHOTS = SnapshotRepository(_snapshot_services)
_JSONL_CACHE = _SNAPSHOTS.jsonl_cache
_SESSION_LEDGER_CACHE = _SNAPSHOTS.session_cache


def read_jsonl(path: Path) -> list[dict]:
    return _SNAPSHOTS.read_jsonl(path)


def _jsonl_data(path: Path) -> dict:
    return _SNAPSHOTS._jsonl_data(path)


def profiles() -> list[dict]:
    return _SNAPSHOTS.profiles()


def _session_ledger_view(fills_path: Path, markouts_path: Path, session_id: str | None) -> dict:
    return _SNAPSHOTS._session_ledger_view(fills_path, markouts_path, session_id)


def build_local_snapshot(profile_id: str | None) -> dict:
    return _SNAPSHOTS.build_local_snapshot(profile_id)


def build_snapshot(profile_id: str | None) -> dict:
    return _SNAPSHOTS.build_snapshot(profile_id)










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


def _monitor_services() -> MonitorServices:
    return MonitorServices(
        LIVE_TEMPLATE=LIVE_TEMPLATE, load_config=load_config,
        resolve_live_credentials=resolve_live_credentials, MarketClient=MarketClient,
        build_fee_model=build_fee_model,
        classify_recent_fill_ownership=classify_recent_fill_ownership,
        DASHBOARD_ACCOUNT_CACHE_SEC=DASHBOARD_ACCOUNT_CACHE_SEC,
        DASHBOARD_FILLS_CACHE_SEC=DASHBOARD_FILLS_CACHE_SEC,
    )


class LiveMonitor(AccountMonitor):
    def __init__(self, session: aiohttp.ClientSession, symbol: str = "SNDK", wallet_id: str = "primary") -> None:
        super().__init__(session, symbol, wallet_id, services=_monitor_services)


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


class BotController(RecoveryControllerMixin):
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



    def _recovery_dependencies(self) -> RecoveryDependencies:
        # Resolve at call time so existing integrations and tests may override
        # dashboard I/O without mutating another module's global namespace.
        return RecoveryDependencies(
            read_json=read_json, atomic_write_json=atomic_write_json,
            _pid_exists=_pid_exists, reconcile_stopped_market=reconcile_stopped_market,
            ROOT=ROOT, AUTO_RECOVERY_DELAY_MS=AUTO_RECOVERY_DELAY_MS,
            AUTO_RECOVERY_WINDOW_MS=AUTO_RECOVERY_WINDOW_MS,
            AUTO_RECOVERY_MAX_ATTEMPTS=AUTO_RECOVERY_MAX_ATTEMPTS,
            SAFE_HARD_RECOVERY_MAX_FILLS=SAFE_HARD_RECOVERY_MAX_FILLS,
            SAFE_HARD_RECOVERY_RETRY_MS=SAFE_HARD_RECOVERY_RETRY_MS,
            _number=_number, _safe_reconciliation_error_label=_safe_reconciliation_error_label,
        )

    def status(self) -> dict:
        value = read_json(self.control_file, {})
        if not isinstance(value, dict):
            value = {}
        attempts = self._load_recovery_attempts()
        pid = int(value.get("pid") or 0)
        running = _pid_exists(pid)
        recovery = self._recovery_state_for_control(value)
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
            "auto_recovery_disabled": value.get("auto_recovery_disabled") is True,
            "recovery": recovery.as_dict(str(value.get("run_id"))) if recovery else None,
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
        _expected_recovery: dict | None = None,
    ) -> dict:
        if strategy not in {"market_maker", "grid"}:
            raise web.HTTPBadRequest(text="策略只能是 market_maker 或 grid")
        async with self._lock:
            if _expected_recovery is not None and not self._automatic_attempt_is_current(_expected_recovery):
                raise web.HTTPConflict(text="自动恢复等待期间已收到停止或新运行意图")
            current = self.status()
            if current["running"]:
                raise web.HTTPConflict(text="已有机器人进程在运行")
            before = read_json(self.control_file, {})
            stop_generation = before.get("stop_generation", 0) if isinstance(before, dict) else 0
            await self._gate_previous_session_summary()
            latest = read_json(self.control_file, {})
            if isinstance(latest, dict) and (
                latest.get("stop_generation", 0) != stop_generation
                or latest.get("run_id") != current.get("run_id")
            ):
                raise web.HTTPConflict(text="对账期间控制状态已变化，已取消本次启动")

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
        # Publish intent before awaiting the start lock: an in-flight official
        # reconciliation must observe cancellation before it creates a child.
        value = read_json(self.control_file, {})
        if not isinstance(value, dict) or not value.get("run_id"):
            return self.status()
        run_id = value["run_id"]
        value["auto_recovery_disabled"] = True
        value["stop_generation"] = int(value.get("stop_generation") or 0) + 1
        value["desired_state"] = "stop_requested"
        value["process_state"] = "stopping"
        value["message"] = "已请求安全撤单退出"
        value["dashboard_heartbeat_ms"] = int(time.time() * 1000)
        atomic_write_json(self.control_file, value)
        async with self._lock:
            value = read_json(self.control_file, {})
            if not isinstance(value, dict) or value.get("run_id") != run_id:
                return self.status()
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


def _http_handlers() -> DashboardHTTP:
    return DashboardHTTP(_http_services)


def _http_services() -> DashboardServices:
    return DashboardServices(
        INDEX_FILE=INDEX_FILE,
        LIVE_TEMPLATE=LIVE_TEMPLATE,
        DASHBOARD_SETTINGS_FILE=DASHBOARD_SETTINGS_FILE,
        LIVE_MARKETS=LIVE_MARKETS,
        build_local_snapshot=build_local_snapshot,
        live_from_bot_summary=live_from_bot_summary,
        read_log_tail=read_log_tail,
        load_config=load_config,
        save_wallet=save_wallet,
        wallet_status=wallet_status,
        configured_wallet_ids=configured_wallet_ids,
        LiveMonitor=LiveMonitor,
        PortfolioController=PortfolioController,
        _merge_dict=_merge_dict,
        apply_tuning_overrides=apply_tuning_overrides,
        load_tuning_overrides=load_tuning_overrides,
        load_dashboard_settings=load_dashboard_settings,
        atomic_write_json=atomic_write_json,
    )


async def index(request: web.Request) -> web.Response:
    return await _http_handlers().index(request)


async def static_asset(request: web.Request) -> web.StreamResponse:
    return await _http_handlers().static_asset(request)


async def api_snapshot(request: web.Request) -> web.Response:
    return await _http_handlers().api_snapshot(request)


def _require_control_token(request: web.Request) -> None:
    return _http_handlers()._require_control_token(request)


async def api_setup(request: web.Request) -> web.Response:
    return await _http_handlers().api_setup(request)


def _budget_edit_reason(controller: PortfolioController, *, read_only: bool = False) -> str:
    return _http_handlers()._budget_edit_reason(controller, read_only=read_only)


def _next_budget_payload(bot: BotController, template: dict) -> dict:
    return _http_handlers()._next_budget_payload(bot, template)


def budget_state(app: web.Application, *, saved_symbol: str | None = None) -> dict:
    return _http_handlers().budget_state(app, saved_symbol=saved_symbol)


async def api_budget(request: web.Request) -> web.Response:
    return await _http_handlers().api_budget(request)


@web.middleware
async def local_origin_only(request: web.Request, handler):
    # A remote page must not acquire the control token through DNS rebinding.
    return await _http_handlers().local_origin_only(request, handler)


async def api_start(request: web.Request) -> web.Response:
    return await _http_handlers().api_start(request)


async def api_recovery_hold(request: web.Request) -> web.Response:
    return await _http_handlers().api_recovery_hold(request)


async def api_market_start(request: web.Request) -> web.Response:
    return await _http_handlers().api_market_start(request)


async def api_market_recovery_hold(request: web.Request) -> web.Response:
    return await _http_handlers().api_market_recovery_hold(request)


async def api_market_stop(request: web.Request) -> web.Response:
    return await _http_handlers().api_market_stop(request)


async def api_grid_settings(request: web.Request) -> web.Response:
    return await _http_handlers().api_grid_settings(request)


async def api_stop(request: web.Request) -> web.Response:
    return await _http_handlers().api_stop(request)


async def app_startup(app: web.Application) -> None:
    session = build_rest_session()
    base = load_config(LIVE_TEMPLATE)
    try:
        wallet_ids = configured_wallet_ids(base) or ["primary"]
    except CredentialStorageError:
        # Keep the local configuration/recovery UI accessible when an encrypted
        # wallet came from another Windows user. No signer is constructed here;
        # /api/setup returns the actionable, non-secret storage error.
        wallet_ids = ["primary"]
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
    app.router.add_get("/assets/{name}", static_asset)
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
