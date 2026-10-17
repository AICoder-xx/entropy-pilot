"""Crash and disk failures exercise real journals; no network or signer is used."""
import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from entropy_bot import storage
from entropy_bot.broker import LiveBroker
from entropy_bot.models import Fill, OrderIntent
from entropy_bot.storage import Ledger, atomic_write_json
from test_ws_and_modify import objects, tracked


@pytest.mark.parametrize("outcome", ["submitted", "send_uncertain", "filled_without_oid"])
def test_process_exits_without_cleanup_and_restart_keeps_intent(tmp_path, outcome):
    # os._exit bypasses context managers, atexit and ledger cleanup.
    worker = """
import os, sys
from pathlib import Path
from entropy_bot.storage import Ledger
ledger = Ledger(Path(sys.argv[1]), 'live', 'market_maker', 'io:SNDK', 'crashed-run')
ledger.record_order_intent({'event':'place','cloid':'synthetic-crash','outcome':sys.argv[2]})
os._exit(73)
"""
    result = subprocess.run(
        [sys.executable, "-X", "utf8", "-c", worker, str(tmp_path), outcome],
        cwd=Path(__file__).resolve().parents[1], capture_output=True, timeout=15,
    )
    assert result.returncode == 73, result.stderr.decode("utf-8", errors="replace")
    restarted = Ledger(tmp_path / "strategies" / "grid", "live", "grid", "io:SNDK", "restart")
    rows = restarted.pending_order_intents()
    assert len(rows) == 1
    assert rows[0][1]["cloid"] == "synthetic-crash"
    assert rows[0][1]["outcome"] == outcome


@pytest.mark.parametrize("operation", ["place", "modify"])
def test_fsync_failure_prevents_sdk_submission(tmp_path, monkeypatch, operation):
    cfg, meta, fees, book = objects()
    broker = LiveBroker(cfg, meta, fees, SimpleNamespace(), "unused", "0x" + "1" * 40)
    broker.exchange = SimpleNamespace(bulk_orders=lambda *_: None, bulk_modify_orders_new=lambda *_: None)
    ledger = Ledger(tmp_path, "live", "market_maker", meta.coin, "disk-fault")
    broker.order_intent_sink = ledger.record_order_intent
    calls = []

    async def forbidden_sdk(*args):
        calls.append(args)
        pytest.fail("An intent that failed fsync must never reach the signer")

    def disk_full(_fd):
        raise OSError("synthetic disk full")

    broker._sdk = forbidden_sdk
    monkeypatch.setattr(storage.os, "fsync", disk_full)
    intent = OrderIntent("mm:bid:0", True, 99, 1)
    if operation == "modify":
        broker.orders_by_role[intent.role] = tracked(intent.role, "0x" + "1" * 32, is_buy=True, oid=7)
    with pytest.raises(OSError, match="synthetic disk full"):
        asyncio.run(broker.place([intent], book) if operation == "place"
                    else broker.modify_orders([(intent.role, intent)], book))
    assert not calls
    # Even a partially successful OS write is treated as uncertain on restart.
    assert len(Ledger(tmp_path, "live", "grid", meta.coin, "restart").pending_order_intents()) == 1


@pytest.mark.parametrize("failure", ["fsync", "replace"])
def test_atomic_save_failure_preserves_original_and_removes_temporary_file(tmp_path, monkeypatch, failure):
    path = tmp_path / "settings.json"
    original = b'{"fixed_budget":12}\n'
    path.write_bytes(original)

    def fail(*_args):
        raise OSError("synthetic storage failure")

    monkeypatch.setattr(storage.os, failure, fail)
    with pytest.raises(OSError, match="synthetic storage failure"):
        atomic_write_json(path, {"fixed_budget": 99})
    assert path.read_bytes() == original
    assert list(tmp_path.glob(".tmp_*")) == []


def test_torn_journal_tail_does_not_release_unknown_order(tmp_path):
    ledger = Ledger(tmp_path, "live", "market_maker", "io:SNDK", "before-crash")
    ledger.record_order_intent({"event": "place", "cloid": "pending", "outcome": "submitted"})
    with ledger.orders_path.open("ab") as stream:
        stream.write(b'{"event":"place","cloid":"pending","outcome":"rest')
        stream.flush()
        os.fsync(stream.fileno())
    before = ledger.orders_path.read_bytes()
    with pytest.raises(RuntimeError, match="UNKNOWN"):
        Ledger(tmp_path, "live", "grid", "io:SNDK", "after-crash").pending_order_intents()
    assert ledger.orders_path.read_bytes() == before
    assert json.loads(before.splitlines()[0])["outcome"] == "submitted"


@pytest.mark.parametrize("strict", [False, True])
def test_corrupt_json_retries_then_preserves_strict_read_boundary(tmp_path, monkeypatch, strict):
    path = tmp_path / "broken.json"
    path.write_text('{"incomplete":', encoding="utf-8")
    waits = []
    monkeypatch.setattr(storage.time, "sleep", waits.append)
    if strict:
        with pytest.raises(json.JSONDecodeError):
            storage.read_json(path, {"fallback": True}, strict=True)
    else:
        assert storage.read_json(path, {"fallback": True}, strict=False) == {"fallback": True}
    assert len(waits) == 5
    assert path.read_text() == '{"incomplete":'


def test_unknown_fill_cost_is_persisted_as_unknown(tmp_path):
    ledger = Ledger(tmp_path, "live", "market_maker", "io:SNDK", "unknown-fee")
    row = Fill("synthetic-fill", "mm:bid:0", "synthetic-cloid", 1, True, 100, 1,
               0.0, "maker", 1000, fee_known=False, realized_pnl_known=False)
    ledger.record_fill(row, "live", "market_maker", "io:SNDK",
                       mid_at_record=100, account_equity_after=100)
    persisted = json.loads(ledger.fills_path.read_text())
    for field in ("fee_usd", "realized_pnl_delta", "actual_booked_wear_usd",
                  "projected_booked_wear_usd", "projected_rebate_total_usd"):
        assert persisted[field] is None
