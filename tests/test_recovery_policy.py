"""Offline recovery acceptance: classification, durable identity and no replay."""
import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import aiohttp
import pytest

import dashboard
from entropy_bot.config import load_config
from entropy_bot.engine import Engine
from entropy_bot.recovery import (
    FailureCode, RecoveryState, classify_exception, failure_record_path,
    recovery_block_message,
)
from entropy_bot.storage import Ledger, OrderIntentJournalError


@pytest.mark.parametrize("status", [408, 425, 429, 500, 503, 599])
def test_transport_response_statuses_are_typed(status):
    exc = aiohttp.ClientResponseError(None, (), status=status, message="arbitrary wording")
    assert classify_exception(exc) is FailureCode.TRANSPORT_UNAVAILABLE


@pytest.mark.parametrize("status", [400, 401, 403, 404, 409, 422])
def test_http_authentication_and_protocol_failures_never_recover(status):
    exc = aiohttp.ClientResponseError(None, (), status=status, message="Cannot connect to host")
    expected = FailureCode.AUTHORIZATION_FAILED if status in {401, 403} else FailureCode.PROTOCOL_FAILURE
    assert classify_exception(exc) is expected
    assert not RecoveryState(expected, True).automatic_recovery_allowed


@pytest.mark.parametrize("exception", [
    asyncio.TimeoutError(), aiohttp.ClientConnectionError("new display wording"),
    aiohttp.ServerDisconnectedError(),
])
def test_direct_transport_types_need_no_message(exception):
    assert classify_exception(exception) is FailureCode.TRANSPORT_UNAVAILABLE


@pytest.mark.parametrize("exception", [
    aiohttp.ClientSSLError(None, OSError("TLS")),
    aiohttp.ServerFingerprintMismatch(b"expected", b"actual", "invalid", 443),
])
def test_tls_identity_failure_is_not_retryable_transport(exception):
    assert classify_exception(exception) is FailureCode.PROTOCOL_FAILURE


def test_wrapped_timeout_cannot_launder_a_programming_or_business_error():
    exc = RuntimeError("连续主循环错误: Cannot connect to host; TimeoutError")
    exc.__cause__ = asyncio.TimeoutError()
    exc.__context__ = aiohttp.ClientConnectionError()
    assert classify_exception(exc) is FailureCode.UNEXPECTED_EXCEPTION
    assert classify_exception(ValueError("new error")) is FailureCode.UNEXPECTED_EXCEPTION


@pytest.mark.parametrize("code", list(FailureCode))
def test_explicit_error_codes_round_trip_and_only_clean_transport_is_recoverable(code):
    exc = RuntimeError("a translated display message")
    exc.code = code.value
    assert classify_exception(exc) is code
    state = RecoveryState(code, True)
    saved = state.as_dict("run-a")
    assert RecoveryState.from_record(saved, run_id="run-a") == state
    assert state.automatic_recovery_allowed is (code is FailureCode.TRANSPORT_UNAVAILABLE)
    assert (recovery_block_message(state) is None) is state.automatic_recovery_allowed


@pytest.mark.parametrize("code", ["NEW_UNKNOWN_ERROR", {}, [], 123])
def test_unknown_error_code_does_not_become_transient(code):
    exc = RuntimeError("官方连接中断")
    exc.code = code
    assert classify_exception(exc) is FailureCode.UNEXPECTED_EXCEPTION


@pytest.mark.parametrize("change", [
    {"schema_version": None}, {"schema_version": True}, {"schema_version": 2},
    {"run_id": "previous-run"}, {"orders_reconciled": 1},
    {"orders_reconciled": "true"}, {"startup_read_only": 0},
    {"startup_read_only": None}, {"code": "FUTURE_CODE"}, {"code": []},
])
def test_old_or_malformed_persisted_recovery_records_are_rejected(change):
    record = RecoveryState(FailureCode.TRANSPORT_UNAVAILABLE, True).as_dict("run-a")
    record.update(change)
    assert RecoveryState.from_record(record, run_id="run-a") is None


@pytest.mark.parametrize("record", [None, [], {}, "TRANSPORT_UNAVAILABLE"])
def test_missing_structured_evidence_never_allows_recovery(record):
    assert RecoveryState.from_record(record, run_id="run-a") is None
    assert recovery_block_message(None)


def test_empty_run_and_unreconciled_orders_remain_blocked():
    state = RecoveryState(FailureCode.TRANSPORT_UNAVAILABLE)
    assert RecoveryState.from_record(state.as_dict(""), run_id="") is None
    assert not state.automatic_recovery_allowed
    assert recovery_block_message(state)


def test_child_evidence_path_cannot_escape_control_directory(tmp_path):
    control = tmp_path / "market.json"
    assert failure_record_path(control, "../../outside").parent == tmp_path
    assert failure_record_path(control, "one") != failure_record_path(control, "two")
    assert failure_record_path(control, "one") == failure_record_path(control, "one")


def controller_fixture(tmp_path, monkeypatch, *, code=FailureCode.TRANSPORT_UNAVAILABLE, clean=True):
    now_ms = 5_000_000
    monkeypatch.setattr(dashboard.time, "time", lambda: now_ms / 1000)
    monkeypatch.setattr(dashboard, "_pid_exists", lambda _pid: False)
    monkeypatch.setattr(dashboard, "GRID_SETTINGS_FILE", tmp_path / "no-grid-settings.json")
    control = tmp_path / "control.json"
    runtime = tmp_path / "runtime.json"
    summary_path = tmp_path / "live_market_maker_io_SNDK_summary.json"
    control.write_text(json.dumps({
        "run_id": "current-run", "summary_session_id": "current-run",
        "desired_state": "halted", "process_state": "halted",
        "strategy": "market_maker", "started_at_ms": now_ms - 120_000,
        "risk_profile": {},
    }), encoding="utf-8")
    runtime.write_text(json.dumps({"runtime": {"state_dir": str(tmp_path)}}), encoding="utf-8")
    summary = {
        "status": "halted", "session_id": "current-run",
        "updated_at_ms": now_ms - 60_000, "session_started_at_ms": now_ms - 120_000,
        "reason": "完全改写的提示文字，含风险停机字样也不参与决策",
        "recovery": RecoveryState(code, clean).as_dict("current-run"),
    }
    summary_path.write_text(json.dumps(summary), encoding="utf-8")
    controller = dashboard.BotController(object(), control_file=control,
        runtime_config=runtime, console_log=tmp_path / "no-log.log")
    return controller, control, summary_path


@pytest.mark.parametrize("field, value", [
    ("recovery", None), ("session_id", "older-run"),
    ("status", "running"), ("updated_at_ms", 1),
])
def test_dashboard_refuses_legacy_stale_or_nonterminal_summary_before_network(tmp_path, monkeypatch, field, value):
    controller, _, path = controller_fixture(tmp_path, monkeypatch)
    summary = json.loads(path.read_text(encoding="utf-8"))
    summary[field] = value
    summary["reason"] = "连续主循环错误: Cannot connect to host"
    path.write_text(json.dumps(summary), encoding="utf-8")

    async def forbidden(*args, **kwargs):
        pytest.fail("unclassified state must not even start official recovery")

    monkeypatch.setattr(dashboard, "reconcile_stopped_market", forbidden)
    controller.start = forbidden
    assert asyncio.run(controller.auto_recover_safe_halt_once()) is False


@pytest.mark.parametrize("code", [c for c in FailureCode if c is not FailureCode.TRANSPORT_UNAVAILABLE])
def test_nontransport_halts_cannot_be_retried_by_dashboard(tmp_path, monkeypatch, code):
    controller, _, _ = controller_fixture(tmp_path, monkeypatch, code=code)

    async def forbidden(*args, **kwargs):
        pytest.fail("risk, unknown and signer failures require human review")

    monkeypatch.setattr(dashboard, "reconcile_stopped_market", forbidden)
    assert asyncio.run(controller.auto_recover_safe_halt_once()) is False


@pytest.mark.parametrize("pending_key", [
    "order_pending_count", "ioc_pending_count", "official_unknown_order_count",
    "official_unknown_fill_count", "official_open_orders", "local_extra_count",
])
def test_pending_order_identity_overrides_optimistic_reconciliation_flag(tmp_path, monkeypatch, pending_key):
    controller, control, _ = controller_fixture(tmp_path, monkeypatch)
    calls = []

    async def reconcile(*args, **kwargs):
        calls.append(kwargs["apply"])
        return {"apply_eligible": True, "reconciliation_ok": True,
                "recoverable_count": 1, pending_key: 1}

    async def forbidden(*args, **kwargs):
        pytest.fail("pending identity must not allow restart")

    monkeypatch.setattr(dashboard, "reconcile_stopped_market", reconcile)
    controller.start = forbidden
    assert asyncio.run(controller.auto_recover_safe_halt_once()) is False
    assert calls == [False]
    assert json.loads(control.read_text(encoding="utf-8"))["safe_hard_recovery_outcome"] == "blocked_unresolved_orders"


def test_rewritten_display_and_dashboard_restart_preserve_classified_recovery(tmp_path, monkeypatch):
    controller, control, _ = controller_fixture(tmp_path, monkeypatch)
    calls = []

    async def reconcile(*args, **kwargs):
        return {"apply_eligible": True, "reconciliation_ok": True, "recoverable_count": 0}

    async def start(*args, **kwargs):
        calls.append((args, kwargs))

    monkeypatch.setattr(dashboard, "reconcile_stopped_market", reconcile)
    controller.start = start
    assert asyncio.run(controller.auto_recover_safe_halt_once()) is True
    replacement = dashboard.BotController(object(), control_file=control,
        runtime_config=controller.runtime_config, console_log=controller.console_log)
    assert replacement.status()["auto_recovery_count"] == 1
    assert replacement.status()["recovery"]["code"] == "TRANSPORT_UNAVAILABLE"
    assert len(calls) == 1


def test_read_only_startup_proof_is_structural_and_run_bound(tmp_path, monkeypatch):
    controller, control, _ = controller_fixture(tmp_path, monkeypatch)
    data = json.loads(control.read_text(encoding="utf-8"))
    controller.console_log.write_text("启动失败: 最近 15 分钟 钱包私钥 wallet.local.json", encoding="utf-8")
    assert not controller._startup_failed_before_signed_mutation(data)
    evidence = failure_record_path(control, "current-run")
    evidence.write_text(json.dumps({"recovery": RecoveryState(
        FailureCode.AUTHORIZATION_FAILED, startup_read_only=True
    ).as_dict("current-run")}), encoding="utf-8")
    assert controller._startup_failed_before_signed_mutation(data)
    data["run_id"] = "new-run"
    assert not controller._startup_failed_before_signed_mutation(data)


def test_engine_startup_failure_evidence_survives_reconstruction(tmp_path, monkeypatch):
    cfg = load_config(Path(__file__).parents[1] / "config.market_maker.json")
    control = tmp_path / "control.json"
    engine = Engine(cfg, control_file=control, run_id="run-evidence")

    async def fail():
        raise ValueError("display text is irrelevant")

    engine.initialize = fail
    with pytest.raises(ValueError):
        asyncio.run(engine.run())
    record = json.loads(failure_record_path(control, "run-evidence").read_text(encoding="utf-8"))
    state = RecoveryState.from_record(record["recovery"], run_id="run-evidence")
    assert state == RecoveryState(FailureCode.UNEXPECTED_EXCEPTION, startup_read_only=True)
    assert not state.automatic_recovery_allowed
    assert not control.exists(), "child evidence must not race the shared control file"


def test_network_recovery_stops_on_auth_failure_during_cancel(tmp_path):
    cfg = load_config(Path(__file__).parents[1] / "config.market_maker.json")
    engine = Engine(cfg)
    engine.market = SimpleNamespace()
    engine.snapshot = SimpleNamespace()
    engine.broker = SimpleNamespace(official_foreign_orders=0)
    engine._write_summary = lambda *args: None
    halts = []

    async def cancel(*args, **kwargs):
        engine._last_exit_failure_code = FailureCode.AUTHORIZATION_FAILED
        return False

    async def halt(reason, *, code):
        halts.append(code)

    engine._cancel_and_reconcile_exit = cancel
    engine.halt = halt
    assert asyncio.run(engine._recover_transient_connectivity(aiohttp.ClientConnectionError())) is False
    assert halts == [FailureCode.AUTHORIZATION_FAILED]


def test_clean_exit_cannot_erase_previous_programming_failure():
    cfg = load_config(Path(__file__).parents[1] / "config.market_maker.json")
    engine = Engine(cfg)
    engine.broker = SimpleNamespace()
    engine.status = "halted"
    engine.recovery_state = RecoveryState(FailureCode.UNEXPECTED_EXCEPTION)
    calls = []

    async def clean(*args, **kwargs):
        calls.append("cancel")
        return True

    engine._cancel_and_reconcile_exit = clean
    asyncio.run(engine._finalize_exit_order_state())
    assert calls == ["cancel"], "unexpected loop failure still needs exit cleanup"
    assert engine.recovery_state == RecoveryState(FailureCode.UNEXPECTED_EXCEPTION, True)
    assert not engine.recovery_state.automatic_recovery_allowed


@pytest.mark.parametrize("cloid", [None, "", "   ", 7, False, []])
def test_journal_known_submission_without_identity_is_unknown_not_empty(tmp_path, cloid):
    ledger = Ledger(tmp_path, "live", "market_maker", "io:SNDK", "synthetic-run")
    ledger.record_order_intent({"event": "place", "cloid": cloid, "outcome": "submitted"})
    before = ledger.orders_path.read_bytes()
    with pytest.raises(OrderIntentJournalError) as caught:
        ledger.pending_order_intents()
    assert classify_exception(caught.value) is FailureCode.ORDER_STATE_UNCERTAIN
    assert ledger.orders_path.read_bytes() == before


def test_manual_start_can_reconcile_old_run_but_automatic_entry_still_cannot(tmp_path, monkeypatch):
    controller, control, path = controller_fixture(tmp_path, monkeypatch)
    summary = json.loads(path.read_text(encoding="utf-8"))
    summary["session_id"] = "previous-engine-run"
    summary.pop("recovery")
    path.write_text(json.dumps(summary), encoding="utf-8")
    calls = []

    async def clean(*args, **kwargs):
        calls.append(kwargs["apply"])
        return {"reconciliation_ok": True, "recoverable_count": 0}

    monkeypatch.setattr(dashboard, "reconcile_stopped_market", clean)
    assert asyncio.run(controller.auto_recover_safe_halt_once()) is False
    assert calls == []
    asyncio.run(controller._gate_previous_session_summary())
    assert calls == [False]
    assert json.loads(control.read_text(encoding="utf-8"))["run_id"] == "current-run"


@pytest.mark.parametrize("pending", ["ioc_pending_count", "order_pending_count"])
def test_manual_old_run_reconciliation_still_blocks_unknown_order(tmp_path, monkeypatch, pending):
    controller, _, path = controller_fixture(tmp_path, monkeypatch)
    summary = json.loads(path.read_text(encoding="utf-8"))
    summary["session_id"] = "previous-engine-run"
    summary.pop("recovery")
    path.write_text(json.dumps(summary), encoding="utf-8")

    async def unresolved(*args, **kwargs):
        assert kwargs["apply"] is False
        return {"reconciliation_ok": True, "recoverable_count": 0, pending: 1}

    monkeypatch.setattr(dashboard, "reconcile_stopped_market", unresolved)
    with pytest.raises(dashboard.web.HTTPConflict):
        asyncio.run(controller._gate_previous_session_summary())


@pytest.mark.parametrize("pause_during_apply", [False, True])
def test_operator_stop_wins_over_inflight_automatic_reconciliation(tmp_path, monkeypatch, pause_during_apply):
    controller, control, _ = controller_fixture(tmp_path, monkeypatch)

    async def scenario():
        entered, release = asyncio.Event(), asyncio.Event()
        calls = []

        async def reconcile(*args, **kwargs):
            apply = kwargs["apply"]
            calls.append(apply)
            if apply is pause_during_apply:
                entered.set()
                await release.wait()
            return {"apply_eligible": True, "reconciliation_ok": True,
                    "recoverable_count": 1 if pause_during_apply and not apply else 0}

        async def forbidden(*args, **kwargs):
            pytest.fail("a stopped recovery attempt must never restart")

        monkeypatch.setattr(dashboard, "reconcile_stopped_market", reconcile)
        controller.start = forbidden
        recovery = asyncio.create_task(controller.auto_recover_safe_halt_once())
        await entered.wait()
        await controller.stop()
        release.set()
        assert await recovery is False
        assert calls == ([False, True] if pause_during_apply else [False])
        # Converging a stop back to halted must not reinstate automatic intent.
        assert await controller.auto_recover_safe_halt_once() is False

    asyncio.run(scenario())
    value = json.loads(control.read_text(encoding="utf-8"))
    assert value["auto_recovery_disabled"] is True
    assert value["stop_generation"] == 1


def test_stop_during_start_gate_prevents_child_creation(tmp_path, monkeypatch):
    controller, control, _ = controller_fixture(tmp_path, monkeypatch)

    async def scenario():
        entered, release = asyncio.Event(), asyncio.Event()

        async def gate():
            entered.set()
            await release.wait()

        def forbidden(*args, **kwargs):
            pytest.fail("stop intent must be checked after awaited startup gate")

        controller._gate_previous_session_summary = gate
        monkeypatch.setattr(dashboard.subprocess, "Popen", forbidden)
        startup = asyncio.create_task(controller.start("market_maker"))
        await entered.wait()
        stop = asyncio.create_task(controller.stop())
        await asyncio.sleep(0)
        assert json.loads(control.read_text(encoding="utf-8"))["auto_recovery_disabled"] is True
        release.set()
        with pytest.raises(dashboard.web.HTTPConflict):
            await startup
        await stop

    asyncio.run(scenario())


def test_readonly_panel_starts_when_wallet_from_another_user_cannot_decrypt(tmp_path, monkeypatch):
    from entropy_bot.credential_store import CredentialStorageError

    session = SimpleNamespace()

    def unavailable(_cfg):
        raise CredentialStorageError("当前用户无法解密；原文件保持不变")

    monkeypatch.setattr(dashboard, "build_rest_session", lambda: session)
    monkeypatch.setattr(dashboard, "configured_wallet_ids", unavailable)
    monkeypatch.setattr(dashboard, "load_dashboard_settings", lambda: {"defaults": {}, "markets": {}})
    app = {"read_only": True}
    asyncio.run(dashboard.app_startup(app))
    assert app["session"] is session
    assert set(app["monitors"]) == {"primary:SNDK", "primary:ANTH"}
    assert "heartbeat_task" not in app and "recovery_task" not in app


@pytest.mark.parametrize("code, message, expected", [
    (FailureCode.RISK_LIMIT, "", False),
    (FailureCode.TRANSPORT_UNAVAILABLE, "风险停机：完全改写展示函数", True),
])
def test_even_empty_or_misleading_presentation_cannot_change_recovery_decision(
    tmp_path, monkeypatch, code, message, expected
):
    import dashboard_recovery

    controller, control, _ = controller_fixture(tmp_path, monkeypatch, code=code)
    value = json.loads(control.read_text(encoding="utf-8"))
    value.update(desired_state="running", process_state="exited", unexpected_exit_at_ms=4_960_000)
    control.write_text(json.dumps(value), encoding="utf-8")
    monkeypatch.setattr(dashboard_recovery, "recovery_block_message", lambda _state: message)
    starts = []

    async def fake_start(*args, **kwargs):
        starts.append(args)

    controller.start = fake_start
    assert asyncio.run(controller.auto_recover_once()) is expected
    assert bool(starts) is expected
