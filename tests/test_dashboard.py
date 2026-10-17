import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

import dashboard
from dashboard import (
    _pid_exists,
    _is_entropy_dashboard_snapshot,
    _safe_reconciliation_error_label,
    calculate_exposure,
    classify_recent_fill_ownership,
    live_from_bot_summary,
    overlay_fresh_bot_telemetry,
)
from entropy_bot.broker import CloidFactory


def test_existing_panel_fingerprint_is_strict_and_local_payload_only():
    assert _is_entropy_dashboard_snapshot(
        {"ok": True, "profiles": [], "controller": {"running": False}}
    )
    assert not _is_entropy_dashboard_snapshot(
        {"ok": True, "controller": {"running": False}}
    )
    assert not _is_entropy_dashboard_snapshot("not-json")


def test_missing_recovery_account_gets_safe_dashboard_label():
    MissingRecoveryAccountError = type(
        "MissingRecoveryAccountError", (RuntimeError,), {}
    )
    assert _safe_reconciliation_error_label(MissingRecoveryAccountError()) == (
        "新目录缺少有效账户配置"
    )
    assert _safe_reconciliation_error_label(RuntimeError("secret")) == (
        "RuntimeError"
    )


def test_named_wallet_profile_supersedes_stale_primary_live_summary(
    monkeypatch, tmp_path
):
    state_dir = tmp_path / "state"
    named_dir = state_dir / "wallets" / "wallet1"
    named_dir.mkdir(parents=True)
    state_dir.mkdir(exist_ok=True)
    (state_dir / "live_market_maker_io_SNDK_summary.json").write_text(
        json.dumps(
            {
                "strategy": "market_maker",
                "coin": "io:SNDK",
                "wallet_id": "primary",
                "status": "stopped",
                "updated_at_ms": 1,
            }
        ),
        encoding="utf-8",
    )
    (named_dir / "live_market_maker_io_SNDK_summary.json").write_text(
        json.dumps(
            {
                "strategy": "market_maker",
                "coin": "io:SNDK",
                "wallet_id": "wallet1",
                "status": "running",
                "updated_at_ms": 2,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(dashboard, "STATE_DIR", state_dir)
    dashboard._JSONL_CACHE.clear()

    available = dashboard.profiles()
    ids = [item["id"] for item in available]
    assert "live_market_maker_io_SNDK" not in ids
    assert "wallets/wallet1/live_market_maker_io_SNDK" in ids

    snapshot = dashboard.build_local_snapshot("live_market_maker_io_SNDK")
    assert snapshot["selected"] == "wallets/wallet1/live_market_maker_io_SNDK"
    assert snapshot["summary"]["status"] == "running"
    assert snapshot["summary"]["wallet_id"] == "wallet1"

    # An already-open browser may still request the old leaf id. Preserve the
    # selected market instead of falling back to the first named-wallet row.
    anth_summary = named_dir / "live_market_maker_io_ANTH_summary.json"
    anth_summary.write_text(
        json.dumps(
            {
                "strategy": "market_maker",
                "coin": "io:ANTH",
                "wallet_id": "wallet1",
                "status": "running",
                "updated_at_ms": 3,
            }
        ),
        encoding="utf-8",
    )
    anth_snapshot = dashboard.build_local_snapshot("live_market_maker_io_ANTH")
    assert anth_snapshot["selected"].endswith("/live_market_maker_io_ANTH")
    assert anth_snapshot["summary"]["coin"] == "io:ANTH"


def test_calculate_exposure_separates_current_and_pending_sides():
    value = calculate_exposure(
        position_qty=0.2,
        mark_px=100.0,
        orders=[
            {"is_buy": True, "remaining_qty": 0.1, "notional_usd": 10.0},
            {"is_buy": False, "remaining_qty": 0.4, "notional_usd": 40.0},
        ],
    )

    assert value["current_long_usd"] == 20.0
    assert value["current_short_usd"] == 0.0
    assert value["pending_buy_usd"] == 10.0
    assert value["pending_sell_usd"] == 40.0
    assert value["worst_case_long_usd"] == pytest.approx(30.0)
    assert value["worst_case_short_usd"] == pytest.approx(20.0)


def test_fresh_bot_telemetry_overlays_slow_official_snapshot(monkeypatch):
    monkeypatch.setattr(dashboard.time, "time", lambda: 100.0)
    live = {
        "ok": True,
        "updated_at_ms": 10_000,
        "monitored_equity_usd": 900.0,
        "position": {"qty": 0.0, "leverage": {"value": 3}},
        "market": {"mark_px": 90.0},
        "open_orders": [],
    }
    summary = {
        "status": "running",
        "updated_at_ms": 99_000,
        "equity_actual": 998.0,
        "position_qty": 0.2,
        "entry_px": 100.0,
        "mark_px": 101.0,
        "best_bid": 100.9,
        "best_ask": 101.1,
        "open_order_details": [
            {
                "is_buy": True,
                "remaining_qty": 0.1,
                "notional_usd": 10.09,
            }
        ],
    }

    value = overlay_fresh_bot_telemetry(live, summary)

    assert value["updated_at_ms"] == 10_000
    assert value["local_telemetry_updated_at_ms"] == 99_000
    assert value["monitored_equity_usd"] == 998.0
    assert value["position"]["qty"] == 0.2
    assert value["position"]["leverage"]["value"] == 3
    assert value["market"]["best_bid"] == 100.9
    assert len(value["open_orders"]) == 1
    assert value["exposure"]["worst_case_long_usd"] == pytest.approx(30.3)


def test_running_view_can_be_built_without_official_rest():
    now_ms = int(dashboard.time.time() * 1000)
    value = live_from_bot_summary(
        {
            "status": "running",
            "updated_at_ms": now_ms,
            "coin": "io:SNDK",
            "account_abstraction": "unifiedAccount",
            "collateral_source": "unified_spot_usdc",
            "equity_actual": 998.0,
            "withdrawable": 800.0,
            "position_qty": -0.1,
            "entry_px": 100.0,
            "mark_px": 101.0,
            "best_bid": 100.9,
            "best_ask": 101.1,
            "growth_mode": True,
            "isolated_only": True,
            "target_leverage": 3,
            "open_order_details": [],
        }
    )

    assert value["official_snapshot_paused"] is True
    assert value["updated_at_ms"] == 0
    assert value["local_telemetry_updated_at_ms"] == now_ms
    assert value["telemetry_stale"] is False
    assert value["position"]["leverage"]["value"] == 3
    assert value["exposure"]["current_short_usd"] == pytest.approx(10.1)


def test_stopped_fill_ownership_queries_unique_recent_orders_only():
    bot_cloid = CloidFactory().next()
    statuses = {
        101: {
            "status": "order",
            "order": {"status": "filled", "order": {"cloid": bot_cloid}},
        },
        202: {
            "status": "order",
            "order": {"status": "filled", "order": {"cloid": "0x1234"}},
        },
        303: None,
    }

    class FakeMarket:
        def __init__(self):
            self.calls = []

        async def order_status(self, _address, oid):
            self.calls.append(oid)
            return statuses[oid]

    market = FakeMarket()
    cache = {}
    rows = [
        {"coin": "io:SNDK", "oid": 101, "time": 9_500},
        {"coin": "io:SNDK", "oid": 101, "time": 9_600},
        {"coin": "io:SNDK", "oid": 202, "time": 9_700},
        {"coin": "io:SNDK", "oid": 303, "time": 9_800},
        {"coin": "io:SNDK", "oid": 404, "time": 1},
    ]
    result = asyncio.run(
        classify_recent_fill_ownership(
            market,  # type: ignore[arg-type]
            "redacted",
            rows,
            coin="io:SNDK",
            cache=cache,
            now_ms=10_000,
            lookback_sec=1,
        )
    )

    assert market.calls == [101, 202, 303]
    assert result == {101: "bot", 202: "external", 303: "unknown"}
    assert cache == {101: "bot", 202: "external"}


@pytest.mark.skipif(os.name != "nt", reason="Windows-specific regression")
def test_pid_probe_never_calls_os_kill_on_windows(monkeypatch):
    def forbidden_kill(*_args, **_kwargs):
        raise AssertionError("Windows process probes must not call os.kill")

    monkeypatch.setattr(dashboard.os, "kill", forbidden_kill)

    assert _pid_exists(os.getpid()) is True
    assert _pid_exists(0) is False


@pytest.mark.skipif(os.name != "nt", reason="Windows-specific regression")
def test_pid_probe_does_not_terminate_target_process():
    process = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(30)"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        assert _pid_exists(process.pid) is True
        assert process.poll() is None
    finally:
        if process.poll() is None:
            process.terminate()
        process.wait(timeout=5)


def test_controller_builds_grid_runtime_without_embedding_wallet_secret(
    monkeypatch, tmp_path
):
    class Monitor:
        async def snapshot(self, *, force=False):
            raise AssertionError("dashboard start must not consume official REST quota")

    class FakeProcess:
        pid = 4242

        def poll(self):
            return None

    launched = {}

    def fake_popen(args, **kwargs):
        launched["args"] = args
        launched["kwargs"] = kwargs
        return FakeProcess()

    runtime_config = tmp_path / "runtime.json"
    control_file = tmp_path / "control.json"
    console_log = tmp_path / "console.log"
    monkeypatch.setattr(dashboard, "RUNTIME_CONFIG", runtime_config)
    monkeypatch.setattr(dashboard, "CONTROL_FILE", control_file)
    monkeypatch.setattr(dashboard, "BOT_CONSOLE_LOG", console_log)
    monkeypatch.setattr(dashboard.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(dashboard, "_pid_exists", lambda pid: pid == 4242)

    status = asyncio.run(dashboard.BotController(Monitor()).start("grid"))
    runtime = json.loads(runtime_config.read_text(encoding="utf-8"))

    assert status["running"] is True
    assert status["strategy"] == "grid"
    assert runtime["strategy"] == "grid"
    assert Path(runtime["runtime"]["state_dir"]).parts[-2:] == ("strategies", "grid")
    assert runtime["risk"]["auto_scale_with_equity"] is False
    assert runtime["grid"]["inventory_direction"] == "long"
    assert "private_key" not in runtime.get("live", {})
    assert "account_address" not in runtime.get("live", {})
    assert "--control-file" in launched["args"]
    assert "--run-id" in launched["args"]
    control = json.loads(control_file.read_text(encoding="utf-8"))
    assert control["recovery_hold_until_session_pause"] is False


def test_controller_recovery_hold_is_persisted_for_child_and_status(
    monkeypatch, tmp_path
):
    class Monitor:
        pass

    class FakeProcess:
        pid = 4343

        def poll(self):
            return None

    runtime_config = tmp_path / "runtime.json"
    control_file = tmp_path / "control.json"
    console_log = tmp_path / "console.log"
    monkeypatch.setattr(dashboard.subprocess, "Popen", lambda *args, **kwargs: FakeProcess())
    monkeypatch.setattr(dashboard, "_pid_exists", lambda pid: pid == 4343)
    controller = dashboard.BotController(
        Monitor(),
        runtime_config=runtime_config,
        control_file=control_file,
        console_log=console_log,
    )

    status = asyncio.run(
        controller.start(
            "market_maker",
            recovery_hold_until_session_pause=True,
        )
    )

    control = json.loads(control_file.read_text(encoding="utf-8"))
    assert control["recovery_hold_until_session_pause"] is True
    assert status["recovery_hold_until_session_pause"] is True


def test_auto_recovery_preserves_recovery_hold(monkeypatch, tmp_path):
    now_ms = 2_500_000
    control_file = tmp_path / "control.json"
    runtime_config = tmp_path / "runtime.json"
    console_log = tmp_path / "console.log"
    control_file.write_text(
        json.dumps(
            {
                "run_id": "old-held-run",
                "desired_state": "running",
                "process_state": "exited",
                "strategy": "market_maker",
                "started_at_ms": now_ms - 120_000,
                "unexpected_exit": True,
                "unexpected_exit_at_ms": now_ms - 31_000,
                "pid": 4343,
                "recovery_hold_until_session_pause": True,
            }
        ),
        encoding="utf-8",
    )
    runtime_config.write_text(
        json.dumps({"runtime": {"state_dir": str(tmp_path / "state")}}),
        encoding="utf-8",
    )
    console_log.write_text("", encoding="utf-8")
    monkeypatch.setattr(dashboard.time, "time", lambda: now_ms / 1000)
    monkeypatch.setattr(dashboard, "_pid_exists", lambda _pid: False)
    controller = dashboard.BotController(
        object(),
        runtime_config=runtime_config,
        control_file=control_file,
        console_log=console_log,
    )
    calls = []

    async def fake_start(strategy, *, recovery_hold_until_session_pause=False):
        calls.append((strategy, recovery_hold_until_session_pause))
        return {"running": True}

    controller.start = fake_start
    assert asyncio.run(controller.auto_recover_once()) is True
    assert calls == [("market_maker", True)]


def test_portfolio_live_profiles_keep_three_x_and_bounded_position_risk(monkeypatch):
    # Exercise the legacy base profile explicitly, independent of a developer's
    # local settings or the public package's intentionally small defaults.
    monkeypatch.setattr(dashboard, "load_dashboard_settings", lambda: {"defaults": {}, "markets": {}})
    monitors = {symbol: object() for symbol in dashboard.LIVE_MARKETS}
    portfolio = dashboard.PortfolioController(monitors)

    sndk = portfolio.controllers["SNDK"].runtime_overrides
    anth = portfolio.controllers["ANTH"].runtime_overrides

    assert sndk["market"]["target_leverage"] == 3
    assert sndk["market_maker"]["levels"] == 1
    assert sndk["market_maker"]["order_notional_usd"] == 1800.0
    assert sndk["market_maker"]["vol_spread_mult"] == 0.0
    assert sndk["market_maker"]["half_spread_bps"] == 1.2
    assert sndk["risk"]["max_open_order_usd"] == 4200.0
    assert sndk["risk"]["max_position_usd"] == 3600.0
    # Single-loss lines: incident + session/principal backstops. Daily wear budget off.
    assert sndk["risk"]["max_principal_drawdown_usd"] == 360.0
    assert sndk["risk"]["max_rolling_drawdown_usd"] == 60.0
    assert sndk["risk"]["daily_loss_budget_usd"] == 0.0
    # Unattended stop-loss must be a closed loop: halt also flattens the
    # residual inventory instead of leaving it open until a human arrives.
    assert sndk["risk"]["flatten_on_halt"] is True
    assert sndk["risk"]["auto_scale_with_equity"] is True
    assert sndk["risk"]["auto_scale_max_equity_usd"] == 6000.0
    assert sndk["runtime"]["us_regular_session_pre_open_buffer_min"] == 30
    assert sndk["runtime"]["pause_during_us_regular_session"] is True
    assert sndk["runtime"]["use_ws_user_feeds"] is True
    assert sndk["runtime"]["use_batch_modify"] is True
    assert sndk["runtime"]["auto_reserve_actions"] is False
    assert sndk["runtime"]["reserve_actions_daily_max_usd"] == 0.0

    assert anth["market"]["target_leverage"] == 3
    assert anth["market_maker"]["order_notional_usd"] == 1800.0
    assert anth["market_maker"]["half_spread_bps"] == 2.5
    assert anth["market_maker"]["level_spacing_bps"] == 4.0
    assert anth["risk"]["max_open_order_usd"] == 4200.0
    assert anth["risk"]["max_position_usd"] == 3600.0
    assert anth["risk"]["max_principal_drawdown_usd"] == 360.0
    assert anth["risk"]["flatten_on_halt"] is True
    assert anth["runtime"]["auto_reserve_actions"] is False
    assert anth["runtime"]["reserve_actions_daily_max_usd"] == 0.0
    # Pre-IPO market has no US cash session: quoting continues through RTH.
    assert anth["runtime"]["pause_during_us_regular_session"] is False
    template = json.loads(dashboard.LIVE_TEMPLATE.read_text(encoding="utf-8"))
    assert template["market"]["max_book_age_sec"] == 30.0


# ---------------------------------------------------------------------------
# AI-operator tuning overlay
# ---------------------------------------------------------------------------


def test_tuning_overlay_whitelists_clamps_and_reports(tmp_path, monkeypatch):
    tuning = tmp_path / "tuning.overrides.json"
    tuning.write_text(
        json.dumps(
            {
                "_说明": "ignored",
                "SNDK": {
                    "_note": "ignored",
                    "market_maker.half_spread_bps": 1.4,
                    "market_maker.refresh_bps": 9.9,
                    "risk.max_position_usd": 999999,
                    "market_maker.refresh_sec": "fast",
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(dashboard, "TUNING_FILE", tuning)

    nested, rejected = dashboard.load_tuning_overrides("SNDK")

    assert nested == {
        "market_maker": {"half_spread_bps": 1.4, "refresh_bps": 2.0}
    }
    assert "越界" in rejected["market_maker.refresh_bps"]
    assert "白名单" in rejected["risk.max_position_usd"]
    assert "数值" in rejected["market_maker.refresh_sec"]
    # Other market untouched.
    assert dashboard.load_tuning_overrides("ANTH") == ({}, {})


def test_tuning_overlay_malformed_file_degrades_to_noop(tmp_path, monkeypatch):
    tuning = tmp_path / "tuning.overrides.json"
    tuning.write_text("{not json", encoding="utf-8")
    monkeypatch.setattr(dashboard, "TUNING_FILE", tuning)
    assert dashboard.load_tuning_overrides("SNDK") == ({}, {})


def test_tuning_overlay_keeps_budget_pair_consistent(tmp_path, monkeypatch):
    tuning = tmp_path / "tuning.overrides.json"
    tuning.write_text(
        json.dumps(
            {
                "SNDK": {
                    "runtime.budget_soft_remaining": 3000,
                    "runtime.budget_hard_remaining": 3000,
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(dashboard, "TUNING_FILE", tuning)
    payload = {"runtime": {"budget_soft_remaining": 5000, "budget_hard_remaining": 1500}}

    dashboard.apply_tuning_overrides(payload, "SNDK")

    assert payload["runtime"]["budget_soft_remaining"] == 3000
    # hard >= soft would fail config validation and block the start; the
    # overlay resets it to 30% of soft instead.
    assert payload["runtime"]["budget_hard_remaining"] == 900
    assert "runtime.budget_hard_remaining" in payload["_tuning_rejected"]
    assert payload["_tuning_applied"]["runtime"]["budget_soft_remaining"] == 3000


def test_shipped_tuning_file_is_valid_with_zero_rejected_keys(tmp_path, monkeypatch):
    """Public installs need no personal tuning file; explicit valid keys parse."""
    tuning = tmp_path / "tuning.overrides.json"
    monkeypatch.setattr(dashboard, "TUNING_FILE", tuning)
    assert dashboard.load_tuning_overrides("SNDK") == ({}, {})
    assert dashboard.load_tuning_overrides("ANTH") == ({}, {})
    tuning.write_text(json.dumps({
        "SNDK": {"market_maker.half_spread_bps": 1.5},
        "ANTH": {"market_maker.half_spread_bps": 2.0},
    }), encoding="utf-8")
    nested_sndk, rejected_sndk = dashboard.load_tuning_overrides("SNDK")
    nested_anth, rejected_anth = dashboard.load_tuning_overrides("ANTH")
    assert rejected_sndk == {} and rejected_anth == {}
    assert nested_sndk == {"market_maker": {"half_spread_bps": 1.5}}
    assert nested_anth == {"market_maker": {"half_spread_bps": 2.0}}


def test_multiwallet_portfolio_creates_isolated_market_controllers():
    monitors = {
        f"{wallet}:{symbol}": object()
        for wallet in ("wallet1", "wallet2")
        for symbol in dashboard.LIVE_MARKETS
    }
    portfolio = dashboard.PortfolioController(monitors)

    assert set(portfolio.controllers) == {
        "wallet1:SNDK", "wallet1:ANTH", "wallet2:SNDK", "wallet2:ANTH"
    }
    first = portfolio.controllers["wallet1:SNDK"]
    second = portfolio.controllers["wallet2:SNDK"]
    assert first.runtime_config != second.runtime_config
    assert first.control_file != second.control_file
    assert first.runtime_overrides["runtime"]["state_dir"] != (
        second.runtime_overrides["runtime"]["state_dir"]
    )
    assert first.runtime_overrides["live"]["wallet_id"] == "wallet1"


def test_portfolio_can_start_one_market_in_recovery_hold_without_peer():
    class FakeBot:
        def __init__(self):
            self.running = False
            self.strategy = None
            self.start_calls = []

        def status(self):
            return {
                "running": self.running,
                "desired_state": "running" if self.running else "stopped",
                "process_state": "running" if self.running else "stopped",
                "strategy": self.strategy,
                "risk_profile": {},
            }

        async def start(
            self,
            strategy,
            *,
            recovery_hold_until_session_pause=False,
        ):
            self.start_calls.append(
                (strategy, recovery_hold_until_session_pause)
            )
            self.running = True
            self.strategy = strategy
            return self.status()

    portfolio = dashboard.PortfolioController.__new__(
        dashboard.PortfolioController
    )
    portfolio._lock = asyncio.Lock()
    portfolio.wallet_ids = ["primary"]
    sndk = FakeBot()
    anth = FakeBot()
    portfolio.controllers = {"SNDK": sndk, "ANTH": anth}
    portfolio._write_aggregate_control = lambda: None

    status = asyncio.run(
        portfolio.start_market(
            "primary",
            "ANTH",
            "market_maker",
            recovery_hold_until_session_pause=True,
        )
    )

    assert sndk.start_calls == []
    assert anth.start_calls == [("market_maker", True)]
    assert status["process_state"] == "partial"
    assert status["markets"]["SNDK"]["running"] is False
    assert status["markets"]["ANTH"]["running"] is True


def test_unexpected_exit_preserves_running_intent_for_recovery(monkeypatch, tmp_path):
    class Monitor:
        pass

    class ExitedProcess:
        pid = 4242

        def poll(self):
            return 7

    control_file = tmp_path / "control.json"
    control_file.write_text(
        json.dumps(
            {
                "run_id": "run-1",
                "desired_state": "running",
                "process_state": "running",
                "pid": 4242,
                "message": "机器人进程运行中",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(dashboard, "CONTROL_FILE", control_file)
    monkeypatch.setattr(dashboard, "_pid_exists", lambda _pid: False)

    controller = dashboard.BotController(Monitor())
    controller.process = ExitedProcess()
    controller._heartbeat_once()

    value = json.loads(control_file.read_text(encoding="utf-8"))
    assert value["desired_state"] == "running"
    assert value["process_state"] == "exited"
    assert value["unexpected_exit"] is True
    assert value["last_exit_code"] == 7
    assert "等待诊断恢复" in value["message"]


def test_starting_without_pid_uses_short_heartbeat_grace(monkeypatch, tmp_path):
    class Monitor:
        pass

    now_ms = 1_800_000
    control_file = tmp_path / "control.json"
    control_file.write_text(
        json.dumps(
            {
                "run_id": "run-starting",
                "desired_state": "running",
                "process_state": "starting",
                "started_at_ms": now_ms - 2_000,
                "pid": None,
                "message": "正在启动",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(dashboard, "CONTROL_FILE", control_file)
    monkeypatch.setattr(dashboard.time, "time", lambda: now_ms / 1000)
    monkeypatch.setattr(
        dashboard,
        "_pid_exists",
        lambda _pid: (_ for _ in ()).throw(AssertionError("grace must not probe PID 0")),
    )

    dashboard.BotController(Monitor())._heartbeat_once()

    value = json.loads(control_file.read_text(encoding="utf-8"))
    assert value["desired_state"] == "running"
    assert value["process_state"] == "starting"
    assert value["pid"] is None
    assert "unexpected_exit" not in value
    assert value["dashboard_heartbeat_ms"] == now_ms


def _terminal_control_fixture(tmp_path, *, summary, desired_state="running"):
    control_file = tmp_path / "control.json"
    runtime_config = tmp_path / "runtime.json"
    console_log = tmp_path / "console.log"
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    runtime_config.write_text(
        json.dumps({"runtime": {"state_dir": str(state_dir)}}), encoding="utf-8"
    )
    control = {
        "run_id": "current-run",
        "desired_state": desired_state,
        "process_state": "running" if desired_state == "running" else "stopping",
        "strategy": "market_maker",
        "started_at_ms": 1_000,
        "pid": 4242,
        "summary_session_id": "current-session",
    }
    control_file.write_text(json.dumps(control), encoding="utf-8")
    payload = {
        "session_id": "current-session",
        "session_started_at_ms": 1_100,
        "updated_at_ms": 2_000,
        **summary,
    }
    (state_dir / "live_market_maker_io_SNDK_summary.json").write_text(
        json.dumps(payload), encoding="utf-8"
    )
    console_log.write_text("", encoding="utf-8")
    controller = dashboard.BotController(
        object(),
        runtime_config=runtime_config,
        control_file=control_file,
        console_log=console_log,
    )
    return controller, control_file


def test_stop_converges_to_stopped_only_from_current_clean_summary(
    monkeypatch, tmp_path
):
    controller, control_file = _terminal_control_fixture(
        tmp_path, summary={"status": "stopped", "open_orders": 0}
    )
    monkeypatch.setattr(dashboard, "_pid_exists", lambda _pid: False)

    status = asyncio.run(controller.stop())

    assert status["desired_state"] == "stopped"
    assert status["process_state"] == "stopped"
    assert "官方挂单终态已确认" in str(status["message"])
    persisted = json.loads(control_file.read_text(encoding="utf-8"))
    assert persisted["run_id"] == "current-run"


def test_heartbeat_converges_timed_out_stop_to_current_halt(monkeypatch, tmp_path):
    controller, control_file = _terminal_control_fixture(
        tmp_path,
        desired_state="stop_requested",
        summary={
            "status": "halted",
            "reason": "退出撤单或订单终态未确认干净",
            "open_orders": 1,
        },
    )
    monkeypatch.setattr(dashboard, "_pid_exists", lambda _pid: False)

    controller._heartbeat_once()

    persisted = json.loads(control_file.read_text(encoding="utf-8"))
    assert persisted["desired_state"] == "halted"
    assert persisted["process_state"] == "halted"
    assert "未确认干净" in persisted["message"]


def test_heartbeat_never_uses_previous_session_to_claim_clean_stop(
    monkeypatch, tmp_path
):
    controller, control_file = _terminal_control_fixture(
        tmp_path,
        desired_state="stop_requested",
        summary={"status": "stopped", "open_orders": 0},
    )
    summary_path = tmp_path / "state" / "live_market_maker_io_SNDK_summary.json"
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    payload["session_id"] = "previous-session"
    summary_path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(dashboard, "_pid_exists", lambda _pid: False)

    controller._heartbeat_once()

    persisted = json.loads(control_file.read_text(encoding="utf-8"))
    assert persisted["desired_state"] == "halted"
    assert "当前运行的最终摘要" in persisted["message"]


@pytest.mark.parametrize("old_status", ["stopped", "halted", "running", "paused"])
def test_start_button_auto_applies_bounded_previous_session_recovery(
    monkeypatch, tmp_path, old_status
):
    controller, control_file = _terminal_control_fixture(
        tmp_path,
        desired_state="stopped",
        summary={"status": old_status, "open_orders": 0},
    )
    before = control_file.read_text(encoding="utf-8")
    calls = []

    async def fake_reconcile(
        symbol, *, wallet_id="primary", apply, max_fills
    ):
        calls.append((symbol, apply, max_fills))
        if apply:
            return {
                "reconciliation_ok": True,
                "recoverable_count": 0,
                "local_extra_count": 0,
                "official_unknown_fill_count": 0,
                "official_unknown_order_count": 0,
                "official_open_orders": 0,
                "ioc_pending_count": 0,
                "order_pending_count": 0,
                "external_activity_detected": False,
                "apply_eligible": True,
            }
        return {
            "reconciliation_ok": False,
            "recoverable_count": 1,
            "local_extra_count": 0,
            "official_unknown_fill_count": 0,
            "official_unknown_order_count": 0,
            "official_open_orders": 0,
            "external_activity_detected": False,
            "apply_eligible": True,
        }

    monkeypatch.setattr(dashboard, "reconcile_stopped_market", fake_reconcile)
    monkeypatch.setattr(dashboard, "_pid_exists", lambda _pid: False)

    asyncio.run(controller._gate_previous_session_summary())

    assert calls == [
        ("SNDK", False, dashboard.SAFE_HARD_RECOVERY_MAX_FILLS),
        ("SNDK", True, dashboard.SAFE_HARD_RECOVERY_MAX_FILLS),
    ]
    assert control_file.read_text(encoding="utf-8") == before


def test_start_blocks_crashed_running_session_when_official_order_remains(
    monkeypatch, tmp_path
):
    controller, control_file = _terminal_control_fixture(
        tmp_path,
        desired_state="running",
        summary={"status": "running", "open_orders": 1},
    )
    before = control_file.read_text(encoding="utf-8")
    calls = []

    async def fake_reconcile(
        symbol, *, wallet_id="primary", apply, max_fills
    ):
        calls.append((symbol, apply, max_fills))
        raise RuntimeError("拒绝补录：官方仍有 SNDK 未结订单")

    monkeypatch.setattr(dashboard, "reconcile_stopped_market", fake_reconcile)
    monkeypatch.setattr(dashboard, "_pid_exists", lambda _pid: False)

    with pytest.raises(dashboard.web.HTTPConflict) as exc_info:
        asyncio.run(controller.start("market_maker"))

    assert calls == [("SNDK", False, dashboard.SAFE_HARD_RECOVERY_MAX_FILLS)]
    assert "停机对账不可用" in exc_info.value.text
    assert "python recover_stopped_fills.py" not in exc_info.value.text
    assert control_file.read_text(encoding="utf-8") == before


def test_start_button_allows_clean_reconciliation_with_historical_external_fills(
    monkeypatch, tmp_path
):
    controller, control_file = _terminal_control_fixture(
        tmp_path,
        desired_state="stopped",
        summary={"status": "stopped", "open_orders": 0},
    )
    before = control_file.read_text(encoding="utf-8")

    async def fake_reconcile(
        _symbol, *, wallet_id="primary", apply, max_fills
    ):
        assert apply is False
        assert max_fills == dashboard.SAFE_HARD_RECOVERY_MAX_FILLS
        return {
            "reconciliation_ok": True,
            "recoverable_count": 0,
            "local_extra_count": 0,
            "official_unknown_fill_count": 0,
            "official_unknown_order_count": 0,
            "official_open_orders": 0,
            "ioc_pending_count": 0,
            "order_pending_count": 0,
            "external_activity_detected": True,
            "apply_eligible": True,
        }

    monkeypatch.setattr(dashboard, "reconcile_stopped_market", fake_reconcile)
    monkeypatch.setattr(dashboard, "_pid_exists", lambda _pid: False)

    asyncio.run(controller._gate_previous_session_summary())

    assert control_file.read_text(encoding="utf-8") == before


def test_start_reuses_last_summary_after_read_only_startup_failure(
    monkeypatch, tmp_path
):
    controller, control_file = _terminal_control_fixture(
        tmp_path,
        desired_state="running",
        summary={"status": "stopped", "open_orders": 0},
    )
    control = json.loads(control_file.read_text(encoding="utf-8"))
    control["summary_session_id"] = "failed-start-without-summary"
    control["started_at_ms"] = 1_788_252_000_000
    control_file.write_text(json.dumps(control), encoding="utf-8")
    controller.console_log.write_text(
        "2026-09-01 16:40:00 启动失败: 最近 15 分钟检测到外部历史成交\n",
        encoding="utf-8",
    )

    async def fake_reconcile(
        _symbol, *, wallet_id="primary", apply, max_fills
    ):
        assert apply is False
        return {
            "reconciliation_ok": True,
            "recoverable_count": 0,
            "local_extra_count": 0,
            "official_unknown_fill_count": 0,
            "official_unknown_order_count": 0,
            "official_open_orders": 0,
            "ioc_pending_count": 0,
            "order_pending_count": 0,
        }

    monkeypatch.setattr(dashboard, "reconcile_stopped_market", fake_reconcile)
    monkeypatch.setattr(dashboard, "_pid_exists", lambda _pid: False)

    asyncio.run(controller._gate_previous_session_summary())


def test_start_blocks_when_fill_counters_match_but_ioc_identity_is_pending(
    monkeypatch, tmp_path
):
    controller, control_file = _terminal_control_fixture(
        tmp_path,
        desired_state="running",
        summary={"status": "running", "open_orders": 0},
    )
    before = control_file.read_text(encoding="utf-8")

    async def fake_reconcile(
        _symbol, *, wallet_id="primary", apply, max_fills
    ):
        assert apply is False
        assert max_fills == dashboard.SAFE_HARD_RECOVERY_MAX_FILLS
        return {
            "reconciliation_ok": True,
            "recoverable_count": 0,
            "local_extra_count": 0,
            "official_unknown_fill_count": 0,
            "official_unknown_order_count": 0,
            "official_open_orders": 0,
            "external_activity_detected": False,
            "ioc_pending_count": 1,
        }

    monkeypatch.setattr(dashboard, "reconcile_stopped_market", fake_reconcile)
    monkeypatch.setattr(dashboard, "_pid_exists", lambda _pid: False)

    with pytest.raises(dashboard.web.HTTPConflict) as exc_info:
        asyncio.run(controller.start("market_maker"))

    assert "ioc_pending_count=1" in exc_info.value.text
    assert control_file.read_text(encoding="utf-8") == before


def test_start_blocks_when_ordinary_order_identity_is_pending(
    monkeypatch, tmp_path
):
    controller, control_file = _terminal_control_fixture(
        tmp_path,
        desired_state="running",
        summary={"status": "running", "open_orders": 0},
    )
    before = control_file.read_text(encoding="utf-8")

    async def fake_reconcile(
        _symbol, *, wallet_id="primary", apply, max_fills
    ):
        assert apply is False
        assert max_fills == dashboard.SAFE_HARD_RECOVERY_MAX_FILLS
        return {
            "reconciliation_ok": True,
            "recoverable_count": 0,
            "local_extra_count": 0,
            "official_unknown_fill_count": 0,
            "official_unknown_order_count": 0,
            "official_open_orders": 0,
            "external_activity_detected": False,
            "ioc_pending_count": 0,
            "order_pending_count": 1,
        }

    monkeypatch.setattr(dashboard, "reconcile_stopped_market", fake_reconcile)
    monkeypatch.setattr(dashboard, "_pid_exists", lambda _pid: False)

    with pytest.raises(dashboard.web.HTTPConflict) as exc_info:
        asyncio.run(controller.start("market_maker"))

    assert "order_pending_count=1" in exc_info.value.text
    assert control_file.read_text(encoding="utf-8") == before


def test_transient_exit_auto_recovers_one_market_after_delay(monkeypatch, tmp_path):
    now_ms = 2_000_000
    control_file = tmp_path / "control.json"
    runtime_config = tmp_path / "runtime.json"
    console_log = tmp_path / "console.log"
    control_file.write_text(
        json.dumps(
            {
                "run_id": "old-run",
                "desired_state": "running",
                "process_state": "exited",
                "strategy": "market_maker",
                "started_at_ms": now_ms - 120_000,
                "unexpected_exit": True,
                "unexpected_exit_at_ms": now_ms - 31_000,
                "pid": 4242,
            }
        ),
        encoding="utf-8",
    )
    runtime_config.write_text(
        json.dumps({"runtime": {"state_dir": str(tmp_path / "state")}}),
        encoding="utf-8",
    )
    console_log.write_text("", encoding="utf-8")
    monkeypatch.setattr(dashboard.time, "time", lambda: now_ms / 1000)
    monkeypatch.setattr(dashboard, "_pid_exists", lambda _pid: False)
    controller = dashboard.BotController(
        object(),
        runtime_config=runtime_config,
        control_file=control_file,
        console_log=console_log,
    )
    calls = []

    async def fake_start(strategy):
        calls.append(strategy)
        return {"running": True}

    controller.start = fake_start
    assert asyncio.run(controller.auto_recover_once()) is True
    assert calls == ["market_maker"]
    assert len(controller._recovery_attempts_ms) == 1
    persisted = json.loads(control_file.read_text(encoding="utf-8"))
    assert persisted["auto_recovery_attempts_ms"] == [now_ms]
    assert persisted["auto_recovery_count"] == 1

    # A replacement dashboard process must inherit the rolling breaker window.
    replacement = dashboard.BotController(
        object(),
        runtime_config=runtime_config,
        control_file=control_file,
        console_log=console_log,
    )
    assert replacement.status()["auto_recovery_count"] == 1


def test_persisted_recovery_limit_survives_dashboard_restart(monkeypatch, tmp_path):
    now_ms = 4_000_000
    control_file = tmp_path / "control.json"
    runtime_config = tmp_path / "runtime.json"
    console_log = tmp_path / "console.log"
    control_file.write_text(
        json.dumps(
            {
                "run_id": "old-run",
                "desired_state": "running",
                "process_state": "exited",
                "strategy": "market_maker",
                "started_at_ms": now_ms - 120_000,
                "unexpected_exit": True,
                "unexpected_exit_at_ms": now_ms - 31_000,
                "pid": 4242,
                "auto_recovery_attempts_ms": [
                    now_ms - 30_000,
                    now_ms - 20_000,
                    now_ms - 10_000,
                ],
            }
        ),
        encoding="utf-8",
    )
    runtime_config.write_text(
        json.dumps({"runtime": {"state_dir": str(tmp_path / "state")}}),
        encoding="utf-8",
    )
    console_log.write_text("", encoding="utf-8")
    monkeypatch.setattr(dashboard.time, "time", lambda: now_ms / 1000)
    monkeypatch.setattr(dashboard, "_pid_exists", lambda _pid: False)
    controller = dashboard.BotController(
        object(),
        runtime_config=runtime_config,
        control_file=control_file,
        console_log=console_log,
    )

    assert asyncio.run(controller.auto_recover_once()) is False
    value = json.loads(control_file.read_text(encoding="utf-8"))
    assert value["desired_state"] == "halted"
    assert value["auto_recovery_attempts_ms"] == [
        now_ms - 30_000,
        now_ms - 20_000,
        now_ms - 10_000,
    ]
    assert "3次上限" in value["message"]


def test_risk_halt_blocks_auto_recovery(monkeypatch, tmp_path):
    now_ms = 3_000_000
    control_file = tmp_path / "control.json"
    runtime_config = tmp_path / "runtime.json"
    console_log = tmp_path / "console.log"
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    (state_dir / "live_market_maker_io_SNDK_summary.json").write_text(
        json.dumps({"status": "halted", "reason": "本金累计回撤达到停机线"}),
        encoding="utf-8",
    )
    control_file.write_text(
        json.dumps(
            {
                "run_id": "halted-run",
                "desired_state": "running",
                "process_state": "exited",
                "strategy": "market_maker",
                "started_at_ms": now_ms - 120_000,
                "unexpected_exit": True,
                "unexpected_exit_at_ms": now_ms - 31_000,
                "pid": 4242,
            }
        ),
        encoding="utf-8",
    )
    runtime_config.write_text(
        json.dumps({"runtime": {"state_dir": str(state_dir)}}), encoding="utf-8"
    )
    console_log.write_text("", encoding="utf-8")
    monkeypatch.setattr(dashboard.time, "time", lambda: now_ms / 1000)
    monkeypatch.setattr(dashboard, "_pid_exists", lambda _pid: False)
    controller = dashboard.BotController(
        object(),
        runtime_config=runtime_config,
        control_file=control_file,
        console_log=console_log,
    )

    assert asyncio.run(controller.auto_recover_once()) is False
    value = json.loads(control_file.read_text(encoding="utf-8"))
    assert value["desired_state"] == "halted"
    assert value["process_state"] == "halted"
    assert "本金累计回撤" in value["message"]


def _safe_halt_fixture(tmp_path, monkeypatch, *, now_ms, symbol="SNDK"):
    control_file = tmp_path / "control.json"
    runtime_config = tmp_path / "runtime.json"
    console_log = tmp_path / "console.log"
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    (state_dir / f"live_market_maker_io_{symbol}_summary.json").write_text(
        json.dumps(
            {
                "status": "halted",
                "reason": "连续主循环错误: Cannot connect to host official.invalid",
                "updated_at_ms": now_ms - 60_000,
            }
        ),
        encoding="utf-8",
    )
    control_file.write_text(
        json.dumps(
            {
                "run_id": "network-halt",
                "desired_state": "halted",
                "process_state": "halted",
                "strategy": "market_maker",
            }
        ),
        encoding="utf-8",
    )
    runtime_config.write_text(
        json.dumps({"runtime": {"state_dir": str(state_dir)}}), encoding="utf-8"
    )
    console_log.write_text("", encoding="utf-8")
    monkeypatch.setattr(dashboard.time, "time", lambda: now_ms / 1000)
    monkeypatch.setattr(dashboard, "_pid_exists", lambda _pid: False)
    return control_file, runtime_config, console_log


def test_safe_hard_recovery_recovers_despite_nonzero_position(
    monkeypatch, tmp_path
):
    """A halted bot holding inventory is the dangerous state, not the safe
    one: recovery proceeds (startup reconciliation adopts the clearinghouse
    position) instead of leaving the position unmanaged."""
    now_ms = 5_000_000
    control_file, runtime_config, console_log = _safe_halt_fixture(
        tmp_path, monkeypatch, now_ms=now_ms
    )

    async def fake_reconcile(
        _symbol, *, wallet_id="primary", apply, max_fills
    ):
        assert apply is False
        assert max_fills == 10
        return {
            "apply_eligible": True,
            "recoverable_count": 0,
            "reconciliation_ok": True,
            "external_activity_detected": False,
            "official_position_zero": False,
        }

    monkeypatch.setattr(dashboard, "reconcile_stopped_market", fake_reconcile)
    controller = dashboard.BotController(
        object(),
        runtime_config=runtime_config,
        control_file=control_file,
        console_log=console_log,
    )
    starts = []

    async def fake_start(strategy, *, recovery_hold_until_session_pause=False):
        starts.append((strategy, recovery_hold_until_session_pause))

    controller.start = fake_start  # type: ignore[method-assign]

    assert asyncio.run(controller.auto_recover_safe_halt_once()) is True
    # Default overrides keep the US-session gate -> conservative hold.
    assert starts == [("market_maker", True)]


def test_safe_hard_recovery_starts_24x7_market_without_hold(
    monkeypatch, tmp_path
):
    """A market with no US-session pause would never leave the monitor-only
    hold, so it restarts normally."""
    now_ms = 5_100_000
    control_file, runtime_config, console_log = _safe_halt_fixture(
        tmp_path, monkeypatch, now_ms=now_ms, symbol="ANTH"
    )

    async def fake_reconcile(
        _symbol, *, wallet_id="primary", apply, max_fills
    ):
        return {
            "apply_eligible": True,
            "recoverable_count": 0,
            "reconciliation_ok": True,
            "external_activity_detected": False,
            "official_position_zero": True,
        }

    monkeypatch.setattr(dashboard, "reconcile_stopped_market", fake_reconcile)
    controller = dashboard.BotController(
        object(),
        symbol="ANTH",
        runtime_config=runtime_config,
        control_file=control_file,
        console_log=console_log,
        runtime_overrides={
            "runtime": {"pause_during_us_regular_session": False}
        },
    )
    starts = []

    async def fake_start(strategy, *, recovery_hold_until_session_pause=False):
        starts.append((strategy, recovery_hold_until_session_pause))

    controller.start = fake_start  # type: ignore[method-assign]

    assert asyncio.run(controller.auto_recover_safe_halt_once()) is True
    assert starts == [("market_maker", False)]


def test_safe_transient_hints_match_verbatim_engine_halt_reasons():
    """Pin the coupling between broker/engine halt texts and recovery hints.

    Even a one-character wording difference can prevent recovery. These
    strings are copied verbatim from the RuntimeError messages in
    entropy_bot/broker.py and engine.py; if a message changes there, this
    test must fail until the corresponding hint is updated."""
    verbatim_reasons = [
        "连续主循环错误: 有 1 笔撤单超过 60 秒仍未确认；为避免重复挂单已停机",
        "连续主循环错误: 订单 mm:bid:0 撤销后官方视图持续矛盾超过 60 秒；"
        "为避免重复下单已停机",
        "连续主循环错误: Cannot connect to host api.hyperliquid.xyz:443",
        "连续主循环错误: 官方连接中断，已禁止新挂单",
        "连续主循环错误: 下单结果不确定，已停止且不重试: 2 笔",
    ]
    for reason in verbatim_reasons:
        assert dashboard.BotController._is_safe_transient_halt_reason(reason), reason
    # Account-risk halts must never qualify.
    for reason in [
        "本金累计回撤 $360.00 达到停机线 $360.00",
        "连续主循环错误: 完全未知的新错误",
        "当日亏损预算已用尽（$100.00/$100.00）",
    ]:
        assert not dashboard.BotController._is_safe_transient_halt_reason(reason), reason


def test_safe_hard_recovery_respects_hourly_attempt_limit(
    monkeypatch, tmp_path
):
    now_ms = 5_200_000
    control_file, runtime_config, console_log = _safe_halt_fixture(
        tmp_path, monkeypatch, now_ms=now_ms
    )
    value = json.loads(control_file.read_text(encoding="utf-8"))
    value["auto_recovery_attempts_ms"] = [
        now_ms - 10_000,
        now_ms - 20_000,
        now_ms - 30_000,
    ]
    control_file.write_text(json.dumps(value), encoding="utf-8")

    controller = dashboard.BotController(
        object(),
        runtime_config=runtime_config,
        control_file=control_file,
        console_log=console_log,
    )
    starts = []

    async def fake_start(strategy, **_kwargs):
        starts.append(strategy)

    controller.start = fake_start  # type: ignore[method-assign]

    assert asyncio.run(controller.auto_recover_safe_halt_once()) is False
    persisted = json.loads(control_file.read_text(encoding="utf-8"))
    assert persisted["safe_hard_recovery_outcome"] == "blocked_attempt_limit"
    assert starts == []


def test_safe_hard_recovery_applies_bounded_fills_then_starts_hold(
    monkeypatch, tmp_path
):
    now_ms = 6_000_000
    control_file = tmp_path / "control.json"
    runtime_config = tmp_path / "runtime.json"
    console_log = tmp_path / "console.log"
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    (state_dir / "live_market_maker_io_SNDK_summary.json").write_text(
        json.dumps(
            {
                "status": "halted",
                "reason": "连续主循环错误: Cannot connect to host official.invalid",
                "updated_at_ms": now_ms - 60_000,
            }
        ),
        encoding="utf-8",
    )
    control_file.write_text(
        json.dumps(
            {
                "run_id": "recoverable-network-halt",
                "desired_state": "halted",
                "process_state": "halted",
                "strategy": "market_maker",
            }
        ),
        encoding="utf-8",
    )
    runtime_config.write_text(
        json.dumps({"runtime": {"state_dir": str(state_dir)}}), encoding="utf-8"
    )
    console_log.write_text("", encoding="utf-8")
    monkeypatch.setattr(dashboard.time, "time", lambda: now_ms / 1000)
    monkeypatch.setattr(dashboard, "_pid_exists", lambda _pid: False)
    calls = []

    async def fake_reconcile(
        _symbol, *, wallet_id="primary", apply, max_fills
    ):
        calls.append((apply, max_fills))
        return {
            "apply_eligible": True,
            "recoverable_count": 2 if not apply else 0,
            "reconciliation_ok": apply,
            "external_activity_detected": False,
            "official_position_zero": True,
        }

    monkeypatch.setattr(dashboard, "reconcile_stopped_market", fake_reconcile)
    controller = dashboard.BotController(
        object(),
        runtime_config=runtime_config,
        control_file=control_file,
        console_log=console_log,
    )
    starts = []

    async def fake_start(strategy, *, recovery_hold_until_session_pause=False):
        starts.append((strategy, recovery_hold_until_session_pause))
        return {"running": True}

    controller.start = fake_start  # type: ignore[method-assign]

    assert asyncio.run(controller.auto_recover_safe_halt_once()) is True
    assert calls == [(False, 10), (True, 10)]
    assert starts == [("market_maker", True)]


def test_jsonl_cache_replacement_and_partial_append(tmp_path):
    path = tmp_path / "fills.jsonl"
    dashboard._JSONL_CACHE.clear()
    path.write_text('{"notional_usd":10,"fee_usd":1}\n', encoding="utf-8")
    assert dashboard._jsonl_data(path)["volume"] == 10
    with path.open("ab") as handle:
        handle.write(b'{"notional_usd":20')
    assert len(dashboard.read_jsonl(path)) == 1
    with path.open("ab") as handle:
        handle.write(b',"fee_usd":2}\n')
    assert dashboard._jsonl_data(path)["volume"] == 30
    assert dashboard._jsonl_data(path)["fees"] == 3
    replacement = tmp_path / "replacement.jsonl"
    replacement.write_text('{"notional_usd":40,"fee_usd":4}\n', encoding="utf-8")
    replacement.replace(path)
    assert dashboard._jsonl_data(path)["volume"] == 40
    assert len(dashboard.read_jsonl(path)) == 1


def test_session_ledger_view_reuses_unchanged_rows_and_invalidates_append(tmp_path):
    fills, marks = tmp_path / "fills.jsonl", tmp_path / "marks.jsonl"
    fills.write_text('{"session_id":"s1","fill_id":"1","notional_usd":10,"is_buy":true}\n', encoding="utf-8")
    marks.write_text('', encoding="utf-8")
    first = dashboard._session_ledger_view(fills, marks, "s1")
    assert dashboard._session_ledger_view(fills, marks, "s1") is first
    with fills.open("a", encoding="utf-8") as handle:
        handle.write('{"session_id":"s1","fill_id":"2","notional_usd":20,"is_buy":false}\n')
    second = dashboard._session_ledger_view(fills, marks, "s1")
    assert second is not first
    assert second["session_buy"] == 10
    assert second["session_sell"] == 20


def _grid_settings_controller(monkeypatch, tmp_path, *, qty=0, orders=None, ok=True):
    class Monitor:
        async def snapshot(self, *, force=False):
            assert force is True
            return {"ok": ok, "position": {"qty": qty}, "open_orders": orders or []}
    monkeypatch.setattr(dashboard, "GRID_SETTINGS_FILE", tmp_path / "grid.settings.json")
    monkeypatch.setattr(dashboard, "_pid_exists", lambda pid: False)
    return dashboard.BotController(
        Monitor(), wallet_id="wallet-test", symbol="SNDK",
        runtime_config=tmp_path / "runtime.json", control_file=tmp_path / "control.json",
        runtime_overrides={"runtime": {"state_dir": str(tmp_path / "state")}},
    )


def test_grid_settings_direction_persisted_per_wallet_market(monkeypatch, tmp_path):
    bot = _grid_settings_controller(monkeypatch, tmp_path)
    value = asyncio.run(bot.save_grid_settings({"inventory_direction": "short", "order_notional_usd": 25, "anchor_price": None}))
    assert value["inventory_direction"] == "short"
    saved = json.loads(dashboard.GRID_SETTINGS_FILE.read_text(encoding="utf-8"))
    assert set(saved) == {"wallet-test:SNDK"}
    assert bot.grid_settings()["order_notional_usd"] == 25
    assert not bot.runtime_config.exists()


@pytest.mark.parametrize("qty,orders,ok", [(1, [], True), (0, [{"oid": "test"}], True), (0, [], False)])
def test_grid_direction_rejects_position_pending_order_or_unknown(monkeypatch, tmp_path, qty, orders, ok):
    bot = _grid_settings_controller(monkeypatch, tmp_path, qty=qty, orders=orders, ok=ok)
    with pytest.raises(dashboard.web.HTTPConflict):
        asyncio.run(bot.save_grid_settings({"inventory_direction": "short"}))
    assert not dashboard.GRID_SETTINGS_FILE.exists()


def test_grid_direction_rejects_local_inventory_even_when_exchange_flat(monkeypatch, tmp_path):
    bot = _grid_settings_controller(monkeypatch, tmp_path)
    state = bot._grid_state_dir()
    state.mkdir(parents=True)
    (state / "live_grid_io_SNDK_summary.json").write_text(json.dumps({"grid_accounting": {"status": "ready", "inventory_qty": 1}}), encoding="utf-8")
    with pytest.raises(dashboard.web.HTTPConflict):
        asyncio.run(bot.save_grid_settings({"inventory_direction": "short"}))
    assert not dashboard.GRID_SETTINGS_FILE.exists()


@pytest.mark.parametrize("updates", [{"spacing_bps": float("nan")}, {"anchor_price": float("inf")}, {"levels_each_side": 2.5}, {"order_notional_usd": 0}, {"inventory_direction": "both"}, {"private_key": "never"}])
def test_grid_settings_rejects_invalid_values(monkeypatch, tmp_path, updates):
    bot = _grid_settings_controller(monkeypatch, tmp_path)
    with pytest.raises(dashboard.web.HTTPBadRequest):
        asyncio.run(bot.save_grid_settings(updates))
    assert not dashboard.GRID_SETTINGS_FILE.exists()


def test_grid_restart_does_not_apply_market_maker_recovery(monkeypatch, tmp_path):
    bot = _grid_settings_controller(monkeypatch, tmp_path)
    bot.control_file.write_text(json.dumps({"strategy": "grid"}), encoding="utf-8")
    monkeypatch.setattr(bot, "_current_summary", lambda strategy: {"status": "stopped", "grid_accounting": {"status": "ready", "inventory_qty": 1}})
    async def forbidden(*args, **kwargs):
        raise AssertionError("must not repair grid with market-maker reconciliation")
    monkeypatch.setattr(dashboard, "reconcile_stopped_market", forbidden)
    asyncio.run(bot._gate_previous_session_summary())


@pytest.mark.parametrize("qty", ["0.0000000000001", "-1", "NaN"])
def test_direction_change_reads_durable_sqlite_not_only_summary(monkeypatch, tmp_path, qty):
    import sqlite3
    bot = _grid_settings_controller(monkeypatch, tmp_path)
    folder = bot._grid_state_dir()
    folder.mkdir(parents=True)
    path = folder / "live_grid_io_SNDK_inventory.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE accounting_state (id INTEGER PRIMARY KEY, value TEXT)")
        connection.execute("INSERT INTO accounting_state VALUES (1, ?)", (json.dumps({"qty": qty, "lots": []}),))
    with pytest.raises(dashboard.web.HTTPConflict):
        asyncio.run(bot.save_grid_settings({"inventory_direction": "short"}))
    assert not dashboard.GRID_SETTINGS_FILE.exists()


def test_grid_settings_preserves_corrupt_file(monkeypatch, tmp_path):
    bot = _grid_settings_controller(monkeypatch, tmp_path)
    dashboard.GRID_SETTINGS_FILE.write_text("incomplete", encoding="utf-8")
    with pytest.raises(dashboard.web.HTTPConflict):
        asyncio.run(bot.save_grid_settings({"spacing_bps": 40}))
    assert dashboard.GRID_SETTINGS_FILE.read_text(encoding="utf-8") == "incomplete"


def test_grid_restart_unknown_is_allowed_into_engine_readonly_reconciliation(monkeypatch, tmp_path):
    bot = _grid_settings_controller(monkeypatch, tmp_path)
    bot.control_file.write_text(json.dumps({"strategy": "grid"}), encoding="utf-8")
    monkeypatch.setattr(bot, "_current_summary", lambda strategy: {"status": "stopped", "grid_accounting": {"status": "unknown"}})
    asyncio.run(bot._gate_previous_session_summary())


def test_console_tail_reads_final_lines_with_utf8(tmp_path):
    path = tmp_path / "console.log"
    rows = [f"第{i}条真实测试日志" for i in range(20000)]
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    assert dashboard.read_log_tail(path, 40) == rows[-40:]
    assert dashboard.read_log_tail(path, 0) == []


def test_dashboard_html_history_scope_and_grid_target_association(tmp_path):
    import shutil
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js required for offline dashboard JavaScript regression")
    runner = tmp_path / "dashboard-regression.js"
    runner.write_text(r"""
const fs=require('fs'),vm=require('vm'),assert=require('assert');
const html=fs.readFileSync(process.argv[2],'utf8');
const script=html.split('<script>')[1].split('</script>')[0].replace('refresh();setInterval(refresh,2000);','');
const nodes=new Map();
function el(id){if(nodes.has(id))return nodes.get(id);const x={value:'',hidden:false,disabled:false,textContent:'',options:[],dataset:{},style:{},classList:{toggle(){}},addEventListener(){},reportValidity(){return true}};Object.defineProperty(x,'innerHTML',{get(){return this.html||''},set(v){this.html=v;if(id==='gridTarget'||id==='profile'){this.options=[...v.matchAll(/<option value="([^"]*)">/g)].map(m=>({value:m[1]}));this.value=this.options[0]?.value||''}}});nodes.set(id,x);return x}
const buttons=[Object.assign(el('mm'),{dataset:{strategy:'market_maker'}}),Object.assign(el('grid'),{dataset:{strategy:'grid'}})];
const context=vm.createContext({document:{querySelector:s=>el(s.slice(1)),querySelectorAll:s=>s==='.strategy'?buttons:[]},setInterval(){},setTimeout(){},console,fetch(){throw new Error('offline test must not fetch')}});
vm.runInContext(script,context);
context.stopped={running:false,markets:{SNDK:{wallet_id:'w',symbol:'SNDK',running:false,strategy:'market_maker',run_id:'old',grid_settings:{inventory_direction:'long'}}}};
context.summary={wallet_id:'w',coin:'io:SNDK',strategy:'market_maker',dashboard_profile:{strategy:'market_maker',mode:'live'},status:'stopped',session_id:'old',session_started_at_ms:1000,updated_at_ms:2000,account_session_net_pnl_usd:-10,unrealized_pnl:-3};
context.live={ok:true,coin:'io:SNDK',updated_at_ms:999000,monitored_equity_usd:900,position:{qty:0,unrealized_pnl_usd:99},market:{},exposure:{}};
vm.runInContext('renderControl(stopped);renderLive(live,summary)',context);
assert.match(el('cards').innerHTML,/已停止（下方历史统计）/);
assert.match(el('cards').innerHTML,/上次运行盈亏/);
assert.match(el('cards').innerHTML,/历史摘要/);
assert.doesNotMatch(el('cards').innerHTML,/正常报价|本进程启动以来/);
assert.match(el('cards').innerHTML,/摘要未实现 −\$3/);
assert.match(el('freshBadge').textContent,/官方账户快照/);
vm.runInContext("lastController.markets.SNDK.running=true;lastController.markets.SNDK.run_id='new';summary.status='running'",context);
assert.equal(vm.runInContext('summaryIsCurrent(summary)',context),false);
vm.runInContext("summary.session_id='new'",context);
assert.equal(vm.runInContext('summaryIsCurrent(summary)',context),true);
context.profiles=[{id:'other-grid',mode:'live',strategy:'grid',wallet_id:'other',coin:'io:SNDK'},{id:'mm-sndk',mode:'live',strategy:'market_maker',wallet_id:'w',coin:'io:SNDK'},{id:'grid-sndk',mode:'live',strategy:'grid',wallet_id:'w',coin:'io:SNDK'}];
vm.runInContext("selectedStrategy='grid';lastSnapshot={profiles};selectedProfile='old-anth';syncGridProfile()",context);
assert.equal(vm.runInContext('selectedProfile',context),'grid-sndk');
vm.runInContext("renderGridAccounting({wallet_id:'other',coin:'io:SNDK',strategy:'grid',grid_accounting:{inventory_qty:12345}})",context);
assert.equal(el('gridCards').innerHTML,'');
assert.match(el('gridAccountingNote').textContent,/暂无 w \/ SNDK 的网格账本/);
vm.runInContext("lastSnapshot={profiles:profiles.slice(0,2)};syncGridProfile()",context);
assert.equal(vm.runInContext('selectedProfile',context),'mm-sndk');
vm.runInContext("renderGridAccounting({wallet_id:'w',coin:'io:SNDK',strategy:'grid',updated_at_ms:2000,grid_accounting:{direction:'long',status:'ready'}})",context);
assert.match(el('gridAccountingScope').textContent,/历史账本.*截止/);

assert.equal(vm.runInContext('knownSessionNetFees({fees_paid:null,projected_self_rebate:5})',context),null);
assert.equal(vm.runInContext('knownSessionNetFees({fees_paid:8,fee_accounting_complete:false,projected_self_rebate:5})',context),null);
assert.equal(vm.runInContext('knownSessionNetFees({fees_paid:8,fee_accounting_complete:true,projected_self_rebate:5})',context),3);
vm.runInContext("selectedStrategy='grid';selectedProfile='old-anth';lastController={};lastSnapshot={profiles:profiles.slice(0,2)};gridLoadedTarget='';gridDirty=true;gridDirtyFields.add('gridDirection');refresh=()=>{globalThis.refreshCalls=(globalThis.refreshCalls||0)+1}",context);
el('gridTarget').value='';el('gridTarget').options=[];el('gridDirection').value='short';
context.initial={controller:context.stopped,profiles:context.profiles.slice(0,2),selected:'old-anth',summary:{...context.summary,coin:'io:ANTH'},live:{...context.live,coin:'io:ANTH'},fills:[]};
vm.runInContext('render(initial)',context);
assert.equal(vm.runInContext('selectedProfile',context),'mm-sndk');
assert.equal(el('gridDirection').value,'short');
assert.equal(context.refreshCalls,1);
assert.match(el('gridLedgerStatus').textContent,/暂无 w \/ SNDK 的网格账本/);
assert.doesNotMatch(el('cards').innerHTML,/io:ANTH/);
assert.match(el('statusLine').textContent,/正在读取 w \/ SNDK/);
assert.match(el('orders').innerHTML,/正在读取所选市场/);
assert.equal(el('gridAccounting').hidden,false);
context.aligned={...context.initial,selected:'mm-sndk',summary:{...context.summary,fees_paid:null,fee_accounting_complete:false},live:context.live};
vm.runInContext('render(aligned)',context);
assert.match(el('statusLine').textContent,/io:SNDK/);
assert.match(el('cards').innerHTML,/费用尚未完整核验，净手续费未知/);
assert.equal(el('gridDirection').value,'short');
console.log('history scope and wallet-market grid association PASS');
""", encoding="utf-8")
    result = subprocess.run([node, str(runner), str(dashboard.INDEX_FILE)], capture_output=True, text=True, encoding="utf-8")
    assert result.returncode == 0, result.stdout + result.stderr


def _create_grid_book(bot, *, initialized=False):
    from entropy_bot.grid_accounting import GridAccounting
    folder = bot._grid_state_dir()
    book = GridAccounting(folder / "live_grid_io_SNDK_inventory.sqlite3", "test-grid-account", direction="long")
    if initialized:
        book.initialize(0, 0, time_ms=1000)
    book.close()
    return folder


@pytest.mark.parametrize("initial,direction,qty", [("long", "short", -2), ("short", "long", 2)])
def test_first_grid_adoption_allows_matching_existing_position(monkeypatch, tmp_path, initial, direction, qty):
    bot = _grid_settings_controller(monkeypatch, tmp_path, qty=qty)
    bot.runtime_overrides["grid"] = {"inventory_direction": initial}
    assert bot._is_pristine_grid_setup() is True
    saved = asyncio.run(bot.save_grid_settings({"inventory_direction": direction}))
    assert saved["inventory_direction"] == direction
    assert not bot._grid_state_dir().exists()


def test_first_grid_adoption_accepts_proven_uninitialized_empty_book(monkeypatch, tmp_path):
    import sqlite3
    bot = _grid_settings_controller(monkeypatch, tmp_path, qty=-2)
    folder = _create_grid_book(bot)
    path = folder / "live_grid_io_SNDK_inventory.sqlite3"
    with sqlite3.connect(path) as connection:
        original = connection.execute("SELECT value FROM accounting_state WHERE id=1").fetchone()[0]
    assert bot._is_pristine_grid_setup() is True
    saved = asyncio.run(bot.save_grid_settings({"inventory_direction": "short"}))
    assert saved["inventory_direction"] == "short"
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT value FROM accounting_state WHERE id=1").fetchone()[0] == original


@pytest.mark.parametrize("suffix,content", [
    ("_summary.json", "{}"), ("_state.json", "{}"),
    ("_orders.jsonl", '{"event":"place_intent"}\n'),
    ("_fills.jsonl", '{"fill_id":"old"}\n'),
])
def test_first_grid_adoption_rejects_history_without_inventory_book(monkeypatch, tmp_path, suffix, content):
    bot = _grid_settings_controller(monkeypatch, tmp_path, qty=-2)
    folder = bot._grid_state_dir()
    folder.mkdir(parents=True)
    (folder / ("live_grid_io_SNDK" + suffix)).write_text(content, encoding="utf-8")
    assert bot._is_pristine_grid_setup() is False
    with pytest.raises(dashboard.web.HTTPConflict):
        asyncio.run(bot.save_grid_settings({"inventory_direction": "short"}))
    assert not dashboard.GRID_SETTINGS_FILE.exists()


@pytest.mark.parametrize("reason", ["initialized", "event", "fatal", "pending_intent"])
def test_existing_grid_does_not_adopt_opposite_position_as_first_use(monkeypatch, tmp_path, reason):
    import sqlite3
    bot = _grid_settings_controller(monkeypatch, tmp_path, qty=-2)
    folder = _create_grid_book(bot, initialized=reason == "initialized")
    path = folder / "live_grid_io_SNDK_inventory.sqlite3"
    with sqlite3.connect(path) as connection:
        if reason == "event":
            connection.execute("INSERT INTO accounting_events VALUES (?,?,?,?,?)", ("funding", "existing-event", 1000, "digest", "{}"))
        if reason == "fatal":
            state = json.loads(connection.execute("SELECT value FROM accounting_state WHERE id=1").fetchone()[0])
            state["fatal_reason"] = "官方启动持仓方向与所选网格方向不符"
            connection.execute("UPDATE accounting_state SET value=? WHERE id=1", (json.dumps(state),))
    if reason == "pending_intent":
        (folder / "live_grid_io_SNDK_orders.jsonl").write_text('{"event":"place_intent"}\n', encoding="utf-8")
    assert bot._is_pristine_grid_setup() is False
    with pytest.raises(dashboard.web.HTTPConflict):
        asyncio.run(bot.save_grid_settings({"inventory_direction": "short"}))
    assert not dashboard.GRID_SETTINGS_FILE.exists()


def test_established_flat_grid_can_change_direction_without_wiping_ledger(monkeypatch, tmp_path):
    import sqlite3
    bot = _grid_settings_controller(monkeypatch, tmp_path, qty=0)
    folder = _create_grid_book(bot, initialized=True)
    path = folder / "live_grid_io_SNDK_inventory.sqlite3"
    with sqlite3.connect(path) as connection:
        original = connection.execute("SELECT value FROM accounting_state WHERE id=1").fetchone()[0]
    assert bot._is_pristine_grid_setup() is False
    saved = asyncio.run(bot.save_grid_settings({"inventory_direction": "short"}))
    assert saved["inventory_direction"] == "short"
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT value FROM accounting_state WHERE id=1").fetchone()[0] == original
