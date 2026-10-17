"""Operator Stop must also win while automatic Start waits for its lock."""
import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import dashboard
from test_recovery_policy import controller_fixture


@pytest.mark.parametrize("recovery_kind", ["network_halt", "unexpected_exit"])
def test_stop_during_automatic_start_lock_wait_cannot_create_child(
    tmp_path, monkeypatch, recovery_kind,
):
    controller, control, _summary = controller_fixture(tmp_path, monkeypatch)
    if recovery_kind == "unexpected_exit":
        value = json.loads(control.read_text(encoding="utf-8"))
        value.update(desired_state="running", process_state="exited", unexpected_exit_at_ms=4_900_000)
        control.write_text(json.dumps(value), encoding="utf-8")
    template = tmp_path / "template.json"
    template.write_text(
        (Path(__file__).parents[1] / "config.example.json").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    for attribute, value in (
        ("LIVE_TEMPLATE", template), ("ROOT", tmp_path),
        ("STATE_DIR", tmp_path / "state"), ("LOG_DIR", tmp_path / "logs"),
    ):
        monkeypatch.setattr(dashboard, attribute, value)
    monkeypatch.setattr(dashboard, "apply_tuning_overrides", lambda *_args: None)
    spawns = []

    def fake_spawn(*_args, **_kwargs):
        spawns.append("synthetic child")
        return SimpleNamespace(pid=12345, poll=lambda: None)

    async def clean(*_args, **_kwargs):
        return {"apply_eligible": True, "reconciliation_ok": True, "recoverable_count": 0}

    async def gate():
        pass

    monkeypatch.setattr(dashboard.subprocess, "Popen", fake_spawn)
    monkeypatch.setattr(dashboard, "reconcile_stopped_market", clean)
    controller._gate_previous_session_summary = gate

    async def scenario():
        entered_start = asyncio.Event()
        original_start = controller.start

        async def observe_start(*args, **kwargs):
            entered_start.set()
            return await original_start(*args, **kwargs)

        controller.start = observe_start
        # Models another start/settings operation owning the controller lock.
        # The tested automatic entry and its actual start implementation run.
        await controller._lock.acquire()
        operation = (
            controller.auto_recover_safe_halt_once
            if recovery_kind == "network_halt"
            else controller.auto_recover_once
        )
        recovery = asyncio.create_task(operation())
        await asyncio.wait_for(entered_start.wait(), timeout=2)
        stop = asyncio.create_task(controller.stop())
        await asyncio.sleep(0)
        during_wait = json.loads(control.read_text(encoding="utf-8"))
        assert during_wait["stop_generation"] == 1
        assert during_wait["auto_recovery_disabled"] is True
        controller._lock.release()
        assert await recovery is False
        await stop

    asyncio.run(scenario())
    assert spawns == []
    final = json.loads(control.read_text(encoding="utf-8"))
    assert final["run_id"] == "current-run"
    assert final["auto_recovery_disabled"] is True
    assert final["desired_state"] != "running"
