from __future__ import annotations

import argparse
import json

import main as bot_main


def test_register_control_process_claims_matching_intent(monkeypatch, tmp_path):
    control = tmp_path / "control.json"
    control.write_text(
        json.dumps(
            {
                "run_id": "new-run",
                "desired_state": "running",
                "process_state": "starting",
                "pid": None,
                "unexpected_exit": True,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(bot_main.os, "getpid", lambda: 4321)
    monkeypatch.setattr(bot_main.time, "time", lambda: 1234.5)
    args = argparse.Namespace(control_file=str(control), run_id="new-run")

    assert bot_main.register_control_process(args) is True

    value = json.loads(control.read_text(encoding="utf-8"))
    assert value["pid"] == 4321
    assert value["process_state"] == "starting"
    assert value["dashboard_heartbeat_ms"] == 1_234_500
    assert "unexpected_exit" not in value


def test_register_control_process_rejects_stale_run(tmp_path):
    control = tmp_path / "control.json"
    control.write_text(
        json.dumps({"run_id": "current", "desired_state": "running"}),
        encoding="utf-8",
    )
    args = argparse.Namespace(control_file=str(control), run_id="stale")

    assert bot_main.register_control_process(args) is False
    assert json.loads(control.read_text(encoding="utf-8"))["run_id"] == "current"
