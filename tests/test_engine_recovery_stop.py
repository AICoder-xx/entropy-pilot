"""Offline acceptance for Stop arriving during engine network recovery."""
import asyncio
import json
from pathlib import Path
import time
from types import SimpleNamespace

import aiohttp
import pytest

from entropy_bot.config import load_config
from entropy_bot.engine import Engine


@pytest.mark.parametrize("stage", ["cleanup", "refresh_context", "sync"])
@pytest.mark.parametrize("stop_kind", ["event", "control"])
def test_stop_during_recovery_await_prevents_requote(tmp_path, stage, stop_kind):
    control = tmp_path / "control.json"
    initial = {
        "run_id": "synthetic-review-run", "desired_state": "running",
        "dashboard_heartbeat_ms": int(time.time() * 1000),
    }
    control.write_text(json.dumps(initial), encoding="utf-8")
    engine = Engine(
        load_config(Path(__file__).parents[1] / "config.example.json"),
        control_file=control, run_id=initial["run_id"],
    )
    events = []

    async def scenario():
        entered, release = asyncio.Event(), asyncio.Event()

        async def wait_at(name):
            events.append(name)
            if name == stage:
                entered.set()
                await release.wait()

        async def cleanup(*_args, **_kwargs):
            await wait_at("cleanup")
            return True

        async def refresh():
            await wait_at("refresh_context")

        async def sync(*_args, **_kwargs):
            await wait_at("sync")

        async def reconcile(*_args, **_kwargs):
            events.append("quote_reconcile")

        book = SimpleNamespace(is_fresh=lambda _age: True)
        engine.market = SimpleNamespace(book=book, refresh_context=refresh)
        engine.snapshot = SimpleNamespace(book=book)
        engine.broker = SimpleNamespace(orders_by_role={}, official_foreign_orders=0, sync=sync)
        engine._cancel_and_reconcile_exit = cleanup
        engine._effective_trading_schedule_state = lambda: SimpleNamespace(paused=False)
        engine._risk_reason = lambda **_kwargs: None
        engine._record_fills = lambda: False
        engine._record_due_markouts = lambda: None
        engine._apply_dynamic_sizing = lambda: False
        engine._write_summary = lambda *_args, **_kwargs: None
        engine._reconcile = reconcile
        task = asyncio.create_task(engine._recover_transient_connectivity(
            aiohttp.ClientConnectionError("synthetic offline")
        ))
        await asyncio.wait_for(entered.wait(), timeout=2)
        if stop_kind == "event":
            engine.stop.set()
        else:
            control.write_text(json.dumps({**initial, "desired_state": "stop_requested"}), encoding="utf-8")
        release.set()
        assert await task is False

    asyncio.run(scenario())
    assert "quote_reconcile" not in events
    assert engine.status != "running"
