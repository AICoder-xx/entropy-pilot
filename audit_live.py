from __future__ import annotations

import argparse
import asyncio
import ctypes
import json
import os
import re
import sys
import time
from collections import Counter
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import urlopen

import aiohttp

from entropy_bot.config import load_config
from entropy_bot.credentials import resolve_live_credentials
from entropy_bot.market import MarketClient


if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass


ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG = ROOT / "audit.live.json"
STATE_DIR = ROOT / "state"
LOG_DIR = ROOT / "logs"
HARD_ERROR_RE = re.compile(
    r"\bERROR\b|Traceback|超过\s*60|PermissionError|RuntimeError",
    re.IGNORECASE,
)
RATE_LIMIT_RE = re.compile(
    r"\b429\b|限流|rate[ -]?limit|too many requests|throttl",
    re.IGNORECASE,
)
TIMEOUT_RE = re.compile(r"timeout|timed out|超时", re.IGNORECASE)
RECOVERY_PROGRESS_RE = re.compile(
    r"\bINFO\b.*entropy\.engine\s*\|\s*(?:暂停)?心跳"
    r"|\bINFO\b.*entropy\.engine\s*\|\s*成交\s+"
    r"|订单状态对账恢复|短暂矛盾已恢复",
    re.IGNORECASE,
)
PENDING_RE = re.compile(r"订单状态等待对账|成交明细等待对账")
PENDING_ROLE_RE = re.compile(r"订单\s+(mm:[^\s]+)")
FILL_ROLE_RE = re.compile(r"成交\s+(mm:[^\s]+)")
RECOVERY_ROLE_RE = re.compile(r"订单状态对账恢复: 订单\s+(mm:[^\s]+)")
GENERIC_PENDING_RECOVERY_RE = re.compile(r"已完成订单与开放委托快照的短暂矛盾已恢复")
LOG_TIME_RE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")
LONG_HEX_RE = re.compile(r"(?i)\b0x[a-f0-9]{64}\b")
EVM_ADDRESS_RE = re.compile(r"(?i)\b(0x[a-f0-9]{6})[a-f0-9]{30}([a-f0-9]{4})\b")
URL_AUTH_RE = re.compile(r"(?i)(https?://)[^/\s:@]+:[^@\s/]+@")


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value if value is not None else "0"))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _float(value: Decimal | None) -> float | None:
    return float(value) if value is not None else None


def _canonical_decimal(value: Any) -> str:
    number = _decimal(value)
    if number == 0:
        return "0"
    return format(number.normalize(), "f")


def sanitize_log_line(line: str) -> str:
    line = LONG_HEX_RE.sub("<redacted-hex>", line)
    line = EVM_ADDRESS_RE.sub(r"\1...\2", line)
    return URL_AUTH_RE.sub(r"\1<redacted>@", line)


def read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                try:
                    value = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(value, dict):
                    rows.append(value)
    except OSError:
        pass
    return rows


def fetch_snapshot(base_url: str, profile: str, timeout: float = 10) -> dict[str, Any]:
    query = urlencode({"profile": profile})
    url = f"{base_url.rstrip('/')}/api/snapshot?{query}"
    try:
        with urlopen(url, timeout=timeout) as response:
            value = json.load(response)
    except (OSError, URLError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"本地面板快照读取失败: {type(exc).__name__}") from exc
    if not isinstance(value, dict) or not value.get("ok"):
        raise RuntimeError("本地面板返回了无效快照")
    return value


def _windows_process_alive(pid: int, kernel32: Any) -> bool:
    """Distinguish a live process from an exited process with an open handle."""
    process_query_limited_information = 0x1000
    handle = kernel32.OpenProcess(process_query_limited_information, False, pid)
    if not handle:
        return False
    try:
        exit_code = ctypes.c_ulong()
        if not kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
            return False
        return exit_code.value == 259  # STILL_ACTIVE
    finally:
        kernel32.CloseHandle(handle)


def process_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        return _windows_process_alive(
            pid, ctypes.windll.kernel32  # type: ignore[attr-defined]
        )
    try:
        os.kill(pid, 0)
    except PermissionError:
        # The process exists; this user is simply not allowed to signal it.
        return True
    except OSError:
        return False
    return True


def official_snapshot_issue(
    *,
    rest_deferred: bool,
    live_ok: bool,
    updated_at_ms: int,
    age_sec: float | None,
    max_age_sec: int,
) -> dict[str, str] | None:
    """Classify official-cache health without alarming on intentional yield.

    A running trader owns the API budget.  The audit deliberately reuses the
    trader-fed local snapshot in that state, so ``rest_deferred`` is expected
    operating mode rather than a warning.  Missing or stale official data is
    still actionable whenever a direct official audit was allowed.
    """
    if rest_deferred:
        return None
    if not live_ok or updated_at_ms <= 0:
        return {"severity": "error", "code": "official_snapshot_missing"}
    if age_sec is not None and age_sec > max_age_sec:
        return {"severity": "warning", "code": "official_snapshot_stale"}
    return None


def latest_account_equity(market_results: dict[str, dict[str, Any]]) -> float | None:
    """Return the shared-account equity from the freshest market heartbeat."""
    candidates: list[tuple[int, float]] = []
    for result in market_results.values():
        official = result.get("official") or {}
        value = official.get("equity_usd")
        if value is None:
            continue
        summary = result.get("summary") or {}
        observed_at_ms = max(
            int(official.get("updated_at_ms") or 0),
            int(summary.get("updated_at_ms") or 0),
        )
        candidates.append((observed_at_ms, float(value)))
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[0])[1]
    return True


def fill_key(row: dict[str, Any]) -> tuple[int, int, str, bool]:
    return (
        int(row.get("time_ms") or 0),
        int(row.get("oid") or 0),
        _canonical_decimal(row.get("qty")),
        bool(row.get("is_buy")),
    )


def fill_snapshot_timestamp(
    live: dict[str, Any], *, finality_sec: int = 0
) -> int:
    """Return the timestamp that actually bounds recent_fills freshness.

    New dashboards expose the cache-fetch timestamp directly.  During a
    no-interruption upgrade an older dashboard may still be serving snapshots;
    in that case the newest official fill is the only safe upper bound.  Add
    the finality interval here because ``reconcile_fills`` subtracts it again.
    An empty old cache deliberately returns zero instead of pretending the
    faster account snapshot also refreshed userFills.
    """
    explicit = int(live.get("fills_updated_at_ms") or 0)
    if explicit > 0:
        return explicit
    newest_fill_ms = max(
        (int(row.get("time_ms") or 0) for row in live.get("recent_fills") or []),
        default=0,
    )
    return newest_fill_ms + max(finality_sec, 0) * 1000 if newest_fill_ms else 0


def reconcile_fills(
    local_rows: list[dict[str, Any]],
    official_rows: list[dict[str, Any]],
    *,
    session_id: str,
    session_started_at_ms: int,
    official_updated_at_ms: int,
    finality_sec: int,
    window_sec: int,
) -> dict[str, Any]:
    cutoff_ms = official_updated_at_ms - finality_sec * 1000
    window_started_at_ms = max(
        session_started_at_ms, cutoff_ms - window_sec * 1000
    )
    local_final = [
        row
        for row in local_rows
        if str(row.get("session_id") or "") == session_id
        and window_started_at_ms <= int(row.get("time_ms") or 0) <= cutoff_ms
    ]
    official_final = [
        row
        for row in official_rows
        if window_started_at_ms <= int(row.get("time_ms") or 0) <= cutoff_ms
    ]
    ownership_classified = any("ownership" in row for row in official_final)
    if ownership_classified:
        official_bot = [
            row for row in official_final if row.get("ownership") == "bot"
        ]
        official_external = [
            row for row in official_final if row.get("ownership") == "external"
        ]
        official_unknown = [
            row
            for row in official_final
            if row.get("ownership") not in {"bot", "external"}
        ]
    else:
        # Backward compatibility for an older dashboard during a no-downtime
        # code upgrade.  Once ownership labels exist, unknown is fail-closed.
        official_bot = official_final
        official_external = []
        official_unknown = []
    local_counter = Counter(fill_key(row) for row in local_final)
    official_counter = Counter(fill_key(row) for row in official_bot)
    missing = sum((official_counter - local_counter).values())
    extra = sum((local_counter - official_counter).values())
    unknown = len(official_unknown)
    return {
        "window_started_at_ms": window_started_at_ms,
        "cutoff_ms": cutoff_ms,
        "local_count": len(local_final),
        "official_count": len(official_final),
        "official_bot_count": len(official_bot),
        "official_external_count": len(official_external),
        "official_unknown_count": unknown,
        "ownership_classified": ownership_classified,
        "missing_local": missing,
        "extra_local": extra,
        "ok": missing == 0 and extra == 0 and unknown == 0,
    }


def markout_metrics(
    fills: list[dict[str, Any]],
    markouts: list[dict[str, Any]],
    *,
    baseline_ms: int,
    minimum_samples: int,
) -> dict[str, Any]:
    baseline_fills = [
        row for row in fills if int(row.get("time_ms") or 0) >= baseline_ms
    ]
    fill_by_id = {
        str(row.get("fill_id")): row
        for row in baseline_fills
        if row.get("fill_id") is not None
    }
    rows_30 = [
        row
        for row in markouts
        if int(row.get("horizon_sec") or 0) == 30
        and int(row.get("fill_time_ms") or 0) >= baseline_ms
        and str(row.get("fill_id") or "") in fill_by_id
    ]
    observed_notional = sum(
        (_decimal(fill_by_id[str(row["fill_id"])].get("notional_usd")) for row in rows_30),
        Decimal("0"),
    )
    markout_pnl = sum(
        (_decimal(row.get("markout_pnl_usd")) for row in rows_30), Decimal("0")
    )
    execution_wear = sum(
        (_decimal(row.get("projected_execution_wear_usd")) for row in rows_30),
        Decimal("0"),
    )
    weighted_bps = (
        markout_pnl / observed_notional * Decimal("10000")
        if observed_notional
        else None
    )
    wear_bps = (
        execution_wear / observed_notional * Decimal("10000")
        if observed_notional
        else None
    )
    return {
        "baseline_ms": baseline_ms,
        "fill_count": len(baseline_fills),
        "volume_usd": _float(
            sum((_decimal(row.get("notional_usd")) for row in baseline_fills), Decimal("0"))
        ),
        "samples": len(rows_30),
        "minimum_samples": minimum_samples,
        "sample_gate_met": len(rows_30) >= minimum_samples,
        "observed_notional_usd": _float(observed_notional),
        "weighted_markout_bps": _float(weighted_bps),
        "projected_execution_wear_bps": _float(wear_bps),
    }


def scan_run_log(path: Path, started_at_ms: int, now_ms: int) -> dict[str, Any]:
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        lines = []
    start_text = datetime.fromtimestamp(started_at_ms / 1000).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    first = next(
        (
            index
            for index, line in enumerate(lines)
            if (match := LOG_TIME_RE.match(line)) and match.group(1) >= start_text
        ),
        len(lines),
    )
    current = lines[first:]
    hard_error_events: list[tuple[int | None, str]] = []
    last_timestamp_ms: int | None = None
    last_recovery_progress_ms: int | None = None
    rate_limit_events: list[tuple[int, str]] = []
    timeout_events: list[tuple[int, str]] = []
    for line in current:
        match = LOG_TIME_RE.match(line)
        line_timestamp_ms: int | None = None
        if match:
            try:
                line_timestamp_ms = int(
                    datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S").timestamp()
                    * 1000
                )
                last_timestamp_ms = line_timestamp_ms
            except ValueError:
                line_timestamp_ms = None
        sanitized = sanitize_log_line(line)
        if HARD_ERROR_RE.search(line):
            # Traceback body lines have no timestamp; associate them with the
            # preceding timestamped ERROR so a later heartbeat/fill can prove
            # that the same process recovered.
            hard_error_events.append((last_timestamp_ms, sanitized))
        if line_timestamp_ms is not None and RECOVERY_PROGRESS_RE.search(line):
            last_recovery_progress_ms = line_timestamp_ms
        if line_timestamp_ms is None:
            continue
        observed_ms = line_timestamp_ms
        if RATE_LIMIT_RE.search(line):
            rate_limit_events.append((observed_ms, sanitized))
        if TIMEOUT_RE.search(line):
            timeout_events.append((observed_ms, sanitized))

    def recent_events(events: list[tuple[int, str]]) -> list[str]:
        return [
            line
            for observed_ms, line in events
            if 0 <= now_ms - observed_ms <= 300_000
        ]

    recent_rate_limits = recent_events(rate_limit_events)
    recent_timeouts = recent_events(timeout_events)
    unresolved_hard_errors = [
        line
        for observed_ms, line in hard_error_events
        if observed_ms is None
        or last_recovery_progress_ms is None
        or observed_ms > last_recovery_progress_ms
    ]
    pending_recent: list[str] = []
    pending_total = 0
    pending_resolved_total = 0
    for index, line in enumerate(current):
        if not PENDING_RE.search(line):
            continue
        pending_total += 1
        role_match = PENDING_ROLE_RE.search(line)
        role = role_match.group(1) if role_match else None
        resolved = bool(
            any(
                (
                    role
                    and (fill_match := FILL_ROLE_RE.search(later))
                    and fill_match.group(1) == role
                )
                or (
                    role
                    and (recovery_match := RECOVERY_ROLE_RE.search(later))
                    and recovery_match.group(1) == role
                )
                or GENERIC_PENDING_RECOVERY_RE.search(later)
                for later in current[index + 1 :]
            )
        )
        if resolved:
            pending_resolved_total += 1
        match = LOG_TIME_RE.match(line)
        if not match:
            continue
        try:
            observed_ms = int(
                datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S").timestamp()
                * 1000
            )
        except ValueError:
            continue
        if not resolved and 0 <= now_ms - observed_ms <= 60_000:
            pending_recent.append(sanitize_log_line(line))
    return {
        "run_line_count": len(current),
        "hard_error_count": len(hard_error_events),
        "hard_error_unresolved_count": len(unresolved_hard_errors),
        "hard_error_resolved_count": len(hard_error_events)
        - len(unresolved_hard_errors),
        "hard_errors": [line for _, line in hard_error_events[-10:]],
        "hard_errors_unresolved": unresolved_hard_errors[-10:],
        "last_recovery_progress_at_ms": last_recovery_progress_ms,
        "rate_limit_count": len(rate_limit_events),
        "rate_limit_recent_5m": len(recent_rate_limits),
        "rate_limit_last_at_ms": (
            rate_limit_events[-1][0] if rate_limit_events else None
        ),
        "rate_limit_recent_lines": recent_rate_limits[-5:],
        "timeout_count": len(timeout_events),
        "timeout_recent_5m": len(recent_timeouts),
        "timeout_last_at_ms": timeout_events[-1][0] if timeout_events else None,
        "timeout_recent_lines": recent_timeouts[-5:],
        "pending_total": pending_total,
        "pending_resolved_total": pending_resolved_total,
        "pending_within_60_sec": pending_recent[-10:],
        "tail": [sanitize_log_line(line) for line in current[-8:]],
    }


def _iso_to_ms(value: str) -> int:
    return int(datetime.fromisoformat(value).timestamp() * 1000)


def _safe_slug(value: str) -> str:
    return "".join(
        char.lower() if char.isalnum() else "_" for char in value
    ).strip("_") or "primary"


def _market_paths(
    symbol: str, wallet_id: str = "primary"
) -> tuple[Path, Path, Path, Path]:
    slug = symbol.lower()
    profile_slug = f"live_market_maker_io_{symbol}"
    wallet_slug = _safe_slug(wallet_id)
    if wallet_slug == "primary":
        state_dir = STATE_DIR
        log_dir = LOG_DIR / slug
        runtime_name = f"config.live.ui.{slug}.runtime.json"
    else:
        state_dir = STATE_DIR / "wallets" / wallet_slug
        log_dir = LOG_DIR / "wallets" / wallet_slug / slug
        runtime_name = f"config.live.ui.{wallet_slug}.{slug}.runtime.json"
    return (
        state_dir / f"{profile_slug}_fills.jsonl",
        state_dir / f"{profile_slug}_markouts.jsonl",
        log_dir / "entropy_bot.log",
        ROOT / runtime_name,
    )


def select_current_run_summary(
    summary: dict[str, Any], market_controller: dict[str, Any]
) -> tuple[dict[str, Any], bool, int]:
    """Reject a previous session summary while a new child is starting.

    The dashboard publishes the new control intent before the engine can write
    its first summary.  During that bounded interval the old file must not be
    used for log scoping, positions, PnL, or heartbeat age.  A small tolerance
    covers filesystem timestamp granularity without allowing an older session
    to masquerade as the current process.
    """

    controller_started_at_ms = int(market_controller.get("started_at_ms") or 0)
    summary_started_at_ms = int(summary.get("session_started_at_ms") or 0)
    desired_running = market_controller.get("desired_state") == "running"
    current = not (
        desired_running
        and controller_started_at_ms > 0
        and summary_started_at_ms + 2_000 < controller_started_at_ms
    )
    effective_started_at_ms = max(
        summary_started_at_ms if current else 0,
        controller_started_at_ms if desired_running else 0,
    )
    return (summary if current else {}, current, effective_started_at_ms)


async def fetch_official_views(
    markets: dict[str, dict[str, Any]],
    wallet_ids: dict[str, str] | None = None,
) -> dict[str, dict[str, Any]]:
    """Fetch an explicitly requested official order/fill view.

    The live dashboard deliberately performs no official REST requests while
    trading. Routine 15-minute audits therefore stay local; this path is for a
    paused/stopped, low-pressure independent reconciliation. Credentials are
    resolved locally only to identify the account; the private key is never
    logged, returned, or used to sign an action.
    """
    result: dict[str, dict[str, Any]] = {}
    start_ms = int((time.time() - 24 * 3600) * 1000)
    async with aiohttp.ClientSession(trust_env=False) as session:
        for symbol in markets:
            _, _, _, runtime_config_path = _market_paths(
                symbol, (wallet_ids or {}).get(symbol, "primary")
            )
            cfg = load_config(runtime_config_path)
            credentials = resolve_live_credentials(cfg, require_private_key=False)
            if not credentials.account_address:
                raise RuntimeError(f"{symbol} 审计无法解析账户地址")
            market = MarketClient(cfg.market, session, info_capacity=120)
            open_raw, fill_raw = await asyncio.gather(
                market.open_orders(credentials.account_address),
                market.fills_since(credentials.account_address, start_ms),
            )
            accepted_coins = {
                cfg.market.symbol,
                f"{cfg.market.dex}:{cfg.market.symbol}",
            }
            orders = []
            for raw in open_raw:
                if raw.get("coin") not in accepted_coins:
                    continue
                price = float(raw.get("limitPx") or raw.get("px") or 0)
                remaining = float(raw.get("sz") or 0)
                original = float(raw.get("origSz") or remaining)
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
            fills = []
            for raw in fill_raw:
                if raw.get("coin") not in accepted_coins:
                    continue
                price = float(raw.get("px") or 0)
                qty = float(raw.get("sz") or 0)
                is_buy = str(raw.get("side") or "").upper() == "B"
                fills.append(
                    {
                        "time_ms": int(raw.get("time") or 0),
                        "oid": int(raw.get("oid") or 0),
                        "is_buy": is_buy,
                        "side": "BUY" if is_buy else "SELL",
                        "price": price,
                        "qty": qty,
                        "notional_usd": price * qty,
                        "liquidity": "taker" if raw.get("crossed") else "maker",
                        "fee_usd": float(raw.get("fee") or 0),
                        "closed_pnl_usd": float(raw.get("closedPnl") or 0),
                        "direction": raw.get("dir"),
                    }
                )
            fetched_at_ms = int(time.time() * 1000)
            result[symbol] = {
                "updated_at_ms": fetched_at_ms,
                "fills_updated_at_ms": fetched_at_ms,
                "open_orders": orders,
                "recent_fills": fills,
                "official_snapshot_paused": False,
                "official_snapshot_source": "audit_direct",
            }
    return result


def merge_official_views(
    snapshots: dict[str, dict[str, Any]],
    official: dict[str, dict[str, Any]],
) -> None:
    for symbol, view in official.items():
        snapshot = snapshots[symbol]
        live = dict(snapshot.get("live") or {})
        live.update(view)
        snapshot["live"] = live


def build_market_audit(
    symbol: str,
    settings: dict[str, Any],
    snapshot: dict[str, Any],
    *,
    now_ms: int,
    finality_sec: int,
    window_sec: int,
    max_snapshot_age_sec: int,
    max_summary_age_sec: int,
    minimum_samples: int,
) -> dict[str, Any]:
    raw_summary = snapshot.get("summary") or {}
    live = snapshot.get("live") or {}
    controller = snapshot.get("controller") or {}
    market_controller = (controller.get("markets") or {}).get(symbol) or {}
    summary, summary_current_run, session_started_at_ms = select_current_run_summary(
        raw_summary, market_controller
    )
    wallet_id = str(raw_summary.get("wallet_id") or "primary")
    fills_path, markouts_path, log_path, runtime_config_path = _market_paths(
        symbol, wallet_id
    )
    fills = read_jsonl(fills_path)
    markouts = read_jsonl(markouts_path)
    runtime_config = read_json(runtime_config_path, {})
    official_updated_at_ms = int(live.get("updated_at_ms") or 0)
    fills_updated_at_ms = fill_snapshot_timestamp(live, finality_sec=finality_sec)
    pid = int(market_controller.get("pid") or 0)
    official_rest_deferred = bool(live.get("official_snapshot_paused"))
    # A deferred live view is derived from the bot summary. Hide it together
    # with an old summary instead of reporting stale positions as current.
    live_view = live if (summary_current_run or not official_rest_deferred) else {}
    if official_rest_deferred:
        reconciliation = {
            "ok": True,
            "deferred": True,
            "reason": "official_rest_yielded_to_live_traders",
            "local_count": sum(
                1
                for row in fills
                if str(row.get("session_id") or "")
                == str(summary.get("session_id") or "")
            ),
            "official_count": None,
            "missing_local": [],
            "extra_local": [],
        }
    else:
        reconciliation = reconcile_fills(
            fills,
            list(live.get("recent_fills") or []),
            session_id=str(summary.get("session_id") or ""),
            session_started_at_ms=session_started_at_ms,
            official_updated_at_ms=fills_updated_at_ms,
            finality_sec=finality_sec,
            window_sec=window_sec,
        )
    metrics = markout_metrics(
        fills,
        markouts,
        baseline_ms=_iso_to_ms(str(settings["baseline_started_at"])),
        minimum_samples=minimum_samples,
    )
    log = scan_run_log(log_path, session_started_at_ms, now_ms)
    official_age_sec = (
        max((now_ms - official_updated_at_ms) / 1000, 0)
        if official_updated_at_ms
        else None
    )
    summary_updated_at_ms = int(summary.get("updated_at_ms") or 0)
    summary_age_sec = (
        max((now_ms - summary_updated_at_ms) / 1000, 0)
        if summary_updated_at_ms
        else None
    )
    runtime_mm = runtime_config.get("market_maker") or {}
    effective_risk = summary.get("risk_profile") or {}
    expected_order = _canonical_decimal(settings.get("order_notional_usd"))
    expected_spread = _canonical_decimal(settings.get("half_spread_bps"))
    actual_order = _canonical_decimal(runtime_mm.get("order_notional_usd"))
    actual_spread = _canonical_decimal(runtime_mm.get("half_spread_bps"))
    effective_order = _canonical_decimal(
        effective_risk.get("order_notional_usd")
        if effective_risk.get("order_notional_usd") is not None
        else runtime_mm.get("order_notional_usd")
    )
    adaptive_metrics = summary.get("adaptive_spread_metrics") or {}
    reported_effective_half = adaptive_metrics.get("effective_half_spread_bps")
    effective_half = (
        _canonical_decimal(reported_effective_half)
        if reported_effective_half is not None
        else _decimal(actual_spread)
        + _decimal(summary.get("adaptive_spread_extra_bps"))
    )
    issues: list[dict[str, str]] = []
    desired_running = market_controller.get("desired_state") == "running"
    if desired_running and (
        not market_controller.get("running") or not process_alive(pid)
    ):
        issues.append({"severity": "error", "code": "process_not_running"})
    if not desired_running:
        issues.append({"severity": "warning", "code": "desired_state_not_running"})
    if not reconciliation["ok"]:
        code = (
            "fill_ownership_unknown"
            if reconciliation.get("official_unknown_count")
            else "fill_reconciliation_mismatch"
        )
        issues.append({"severity": "error", "code": code})
    if reconciliation.get("official_external_count"):
        issues.append(
            {"severity": "warning", "code": "external_fills_in_reconciliation_window"}
        )
    if log["hard_error_unresolved_count"]:
        issues.append({"severity": "error", "code": "hard_error_in_current_run"})
    if log["rate_limit_recent_5m"]:
        issues.append({"severity": "warning", "code": "rate_limit_recent"})
    if log["timeout_recent_5m"]:
        issues.append({"severity": "warning", "code": "timeout_recent"})
    snapshot_issue = official_snapshot_issue(
        rest_deferred=official_rest_deferred,
        live_ok=bool(live.get("ok")),
        updated_at_ms=official_updated_at_ms,
        age_sec=official_age_sec,
        max_age_sec=max_snapshot_age_sec,
    )
    if snapshot_issue:
        issues.append(snapshot_issue)
    controller_started_at_ms = int(market_controller.get("started_at_ms") or 0)
    startup_age_sec = (
        max((now_ms - controller_started_at_ms) / 1000, 0)
        if controller_started_at_ms
        else None
    )
    if desired_running and not summary_current_run:
        issues.append(
            {
                "severity": (
                    "warning"
                    if startup_age_sec is not None
                    and startup_age_sec <= max_summary_age_sec
                    else "error"
                ),
                "code": (
                    "current_run_summary_pending"
                    if startup_age_sec is not None
                    and startup_age_sec <= max_summary_age_sec
                    else "local_heartbeat_stale"
                ),
            }
        )
    elif desired_running and (
        summary_age_sec is None or summary_age_sec > max_summary_age_sec
    ):
        issues.append({"severity": "error", "code": "local_heartbeat_stale"})
    if actual_order != expected_order or actual_spread != expected_spread:
        issues.append({"severity": "warning", "code": "audit_baseline_config_mismatch"})
    return {
        "symbol": symbol,
        "run_id": str(market_controller.get("run_id") or ""),
        "pid": pid,
        "process_alive": process_alive(pid),
        "desired_state": market_controller.get("desired_state"),
        "process_state": market_controller.get("process_state"),
        "recovery_hold_until_session_pause": bool(
            market_controller.get("recovery_hold_until_session_pause")
        ),
        "runtime_sec": max((now_ms - session_started_at_ms) / 1000, 0)
        if session_started_at_ms
        else None,
        "config": {
            "order_notional_usd": _float(_decimal(effective_order)),
            "half_spread_bps": _float(_decimal(actual_spread)),
            "adaptive_spread_extra_bps": float(
                summary.get("adaptive_spread_extra_bps") or 0.0
            ),
            "effective_half_spread_bps": _float(
                effective_half
            ),
            "target_leverage": (runtime_config.get("market") or {}).get(
                "target_leverage"
            ),
            "adaptive_spread_metrics": adaptive_metrics,
        },
        "summary": {
            "current_run": summary_current_run,
            "status": summary.get("status") or (
                "starting" if desired_running and not summary_current_run else None
            ),
            "reason": summary.get("reason"),
            "updated_at_ms": summary_updated_at_ms,
            "age_sec": summary_age_sec,
            "fill_count": int(summary.get("fill_count") or 0),
            "volume_usd": float(summary.get("volume_usd") or 0),
            "local_open_orders": int(summary.get("open_orders") or 0),
            "session_net_pnl_usd": float(
                summary.get("account_session_net_pnl_usd") or 0
            ),
            "principal_net_pnl_usd": float(summary.get("principal_net_pnl_usd") or 0),
            "risk_profile": effective_risk,
        },
        "official": {
            "rest_deferred": official_rest_deferred,
            "updated_at_ms": official_updated_at_ms,
            "fills_updated_at_ms": fills_updated_at_ms,
            "snapshot_age_sec": official_age_sec,
            "open_orders": (
                len(live_view.get("open_orders") or []) if live_view else None
            ),
            "equity_usd": (
                float(live_view.get("monitored_equity_usd") or 0)
                if live_view
                else None
            ),
            "position_qty": (
                float(((live_view.get("position") or {}).get("qty")) or 0)
                if live_view
                else None
            ),
            "isolated_leverage": (
                ((live_view.get("position") or {}).get("leverage") or {}).get(
                    "value"
                )
                if live_view
                else None
            ),
        },
        "reconciliation": reconciliation,
        "markout_30": metrics,
        "log": log,
        "issues": issues,
    }


def _order_confirmation_point(snapshot: dict[str, Any]) -> dict[str, int]:
    local_open_orders = int((snapshot.get("summary") or {}).get("open_orders") or 0)
    return {
        "official_updated_at_ms": int(
            (snapshot.get("live") or {}).get("updated_at_ms") or 0
        ),
        "official_open_orders": len(
            (snapshot.get("live") or {}).get("open_orders") or []
        ),
        # 做市模式最多需要确认买卖两边。网格或本地短暂只有一单时，
        # 不会因此要求官方快照必须出现两单。
        "expected_open_orders": min(local_open_orders, 2),
        "fill_count": int((snapshot.get("summary") or {}).get("fill_count") or 0),
    }


def evaluate_order_confirmation(
    first: dict[str, int], second: dict[str, int]
) -> tuple[dict[str, Any], str | None]:
    official_advanced = (
        second["official_updated_at_ms"] > first["official_updated_at_ms"]
    )
    turnover_active = second["fill_count"] > first["fill_count"]
    expected_open_orders = second["expected_open_orders"]
    still_below_expected = (
        expected_open_orders > 0
        and second["official_open_orders"] < expected_open_orders
    )
    still_zero = second["official_open_orders"] == 0
    if turnover_active:
        classification = "active_turnover"
    elif not official_advanced:
        classification = "snapshot_not_advanced"
    elif still_below_expected:
        classification = "needs_recheck"
    else:
        classification = "restored"

    issue_code: str | None = None
    if still_below_expected and not turnover_active:
        coverage = "zero_orders" if still_zero else "one_sided_orders"
        state = "needs_recheck" if official_advanced else "snapshot_not_advanced"
        issue_code = f"official_{coverage}_{state}"

    return (
        {
            "official_time_advanced": official_advanced,
            "turnover_active": turnover_active,
            "expected_open_orders": expected_open_orders,
            "still_below_expected": still_below_expected,
            "still_zero": still_zero,
            "classification": classification,
        },
        issue_code,
    )


def audit(
    config_path: Path,
    *,
    confirm_zero_orders: bool = True,
    official_rest: bool = False,
) -> dict[str, Any]:
    config = read_json(config_path, None)
    if not isinstance(config, dict):
        raise RuntimeError("审计配置读取失败")
    base_url = str(config.get("dashboard_url") or "http://127.0.0.1:8765")
    markets = config.get("markets") or {}
    if not isinstance(markets, dict) or not markets:
        raise RuntimeError("审计配置未定义市场")
    snapshots = {
        symbol: fetch_snapshot(base_url, str(settings["profile"]))
        for symbol, settings in markets.items()
    }
    if official_rest:
        wallet_ids = {
            symbol: str((snapshot.get("summary") or {}).get("wallet_id") or "primary")
            for symbol, snapshot in snapshots.items()
        }
        merge_official_views(
            snapshots,
            asyncio.run(fetch_official_views(markets, wallet_ids)),
        )
    confirmations: dict[str, list[dict[str, Any]]] = {}
    confirmation_symbols = [
        symbol
        for symbol, snapshot in snapshots.items()
        if ((snapshot.get("controller") or {}).get("markets") or {})
        .get(symbol, {})
        .get("running")
        and _order_confirmation_point(snapshot)["expected_open_orders"] > 0
        and _order_confirmation_point(snapshot)["official_open_orders"]
        < _order_confirmation_point(snapshot)["expected_open_orders"]
    ]
    wait_sec = float(
        config.get("confirm_order_coverage_sec")
        or config.get("confirm_zero_orders_sec")
        or 0
    )
    if official_rest and confirm_zero_orders and confirmation_symbols and wait_sec > 0:
        before = snapshots
        time.sleep(wait_sec)
        refreshed_snapshots = {
            symbol: fetch_snapshot(base_url, str(markets[symbol]["profile"]))
            for symbol in confirmation_symbols
        }
        refreshed_official = asyncio.run(
            fetch_official_views(
                {symbol: markets[symbol] for symbol in confirmation_symbols},
                {
                    symbol: str(
                        (refreshed_snapshots[symbol].get("summary") or {}).get(
                            "wallet_id"
                        )
                        or "primary"
                    )
                    for symbol in confirmation_symbols
                },
            )
        )
        merge_official_views(refreshed_snapshots, refreshed_official)
        for symbol in confirmation_symbols:
            refreshed = refreshed_snapshots[symbol]
            confirmations[symbol] = [
                _order_confirmation_point(before[symbol]),
                _order_confirmation_point(refreshed),
            ]
            snapshots[symbol] = refreshed
    now_ms = int(time.time() * 1000)
    market_results = {
        symbol: build_market_audit(
            symbol,
            settings,
            snapshots[symbol],
            now_ms=now_ms,
            finality_sec=int(config.get("finality_sec") or 60),
            window_sec=int(config.get("reconciliation_window_sec") or 900),
            max_snapshot_age_sec=int(config.get("max_snapshot_age_sec") or 60),
            max_summary_age_sec=int(config.get("max_summary_age_sec") or 90),
            minimum_samples=int(config.get("minimum_markout_samples") or 20),
        )
        for symbol, settings in markets.items()
    }
    issues = [
        {"symbol": symbol, **issue}
        for symbol, result in market_results.items()
        for issue in result["issues"]
    ]
    for symbol, history in confirmations.items():
        if len(history) != 2:
            continue
        first, second = history
        classification, issue_code = evaluate_order_confirmation(first, second)
        confirmations[symbol] = [first, second, classification]
        if issue_code:
            issues.append(
                {
                    "symbol": symbol,
                    "severity": "warning",
                    "code": issue_code,
                }
            )
    errors = [issue for issue in issues if issue["severity"] == "error"]
    warnings = [issue for issue in issues if issue["severity"] == "warning"]
    return {
        "ok": not errors,
        "checked_at_ms": now_ms,
        "principal_usd": float(_decimal(config.get("principal_usd"))),
        "account_equity_usd": latest_account_equity(market_results),
        "markets": market_results,
        "zero_order_confirmations": confirmations,
        "errors": errors,
        "warnings": warnings,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Entropy 双市场只读实盘审计（不会下单、撤单、输出私钥或使用私钥签名）"
        )
    )
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument(
        "--no-confirm-wait",
        action="store_true",
        help="测试或手工快速检查时跳过挂单覆盖不足的二次确认等待",
    )
    parser.add_argument("--compact", action="store_true")
    parser.add_argument(
        "--official-rest",
        action="store_true",
        help=(
            "显式执行官方订单/成交对账；机器人运行时默认关闭，"
            "避免与交易链路争抢 /info 限速"
        ),
    )
    args = parser.parse_args()
    try:
        result = audit(
            args.config.resolve(),
            confirm_zero_orders=not args.no_confirm_wait,
            official_rest=args.official_rest,
        )
    except Exception as exc:
        result = {
            "ok": False,
            "checked_at_ms": int(time.time() * 1000),
            "errors": [
                {
                    "severity": "error",
                    "code": "audit_failed",
                    "message": str(exc),
                }
            ],
        }
    print(
        json.dumps(
            result,
            ensure_ascii=False,
            indent=None if args.compact else 2,
            separators=(",", ":") if args.compact else None,
        )
    )
    return 0 if result.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
