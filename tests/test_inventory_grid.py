import asyncio
import json
from dataclasses import replace
from pathlib import Path

import pytest

from entropy_bot.broker import PaperBroker
from entropy_bot.config import ConfigError, load_config
from entropy_bot.engine import Engine
from entropy_bot.instance_lock import InstanceLock
from entropy_bot.models import AccountState, OrderIntent, TrackedOrder
from entropy_bot.storage import Ledger
from entropy_bot.strategies import GridStrategy
from test_strategies import setup


@pytest.mark.parametrize("direction,qty", [("long", 0), ("short", 0), ("long", 1.2), ("short", -1.2)])
def test_orders_are_backed_by_real_directional_inventory(tmp_path, direction, qty):
    cfg, market, fees = setup(tmp_path, "grid")
    cfg = replace(
        cfg,
        # Leave entry room above the explicit 1.2-unit fixture at fair=100.
        risk=replace(cfg.risk, max_position_usd=300.0, max_open_order_usd=1000.0, min_order_notional_usd=10.0),
        grid=replace(cfg.grid, inventory_direction=direction, levels_each_side=3, spacing_bps=10.0, order_notional_usd=50.0),
    )
    items = GridStrategy(cfg, fees).desired(market, AccountState(position_qty=qty))
    opening = [x for x in items if not x.reduce_only]
    closing = [x for x in items if x.reduce_only]
    assert opening and all(x.is_buy == (direction == "long") for x in opening)
    assert all(x.is_buy != (direction == "long") for x in closing)
    assert sum(x.qty for x in closing) <= abs(qty) + 1e-9
    assert sum(x.qty for x in opening) + abs(qty) <= cfg.risk.max_position_usd / market.fair_px + 1e-9
    assert sum(x.notional for x in items) <= cfg.risk.max_open_order_usd + 1e-9
    if qty == 0:
        assert not closing


def test_wrong_direction_is_blocked_and_dust_does_not_recenter(tmp_path):
    cfg, market, fees = setup(tmp_path, "grid")
    strategy = GridStrategy(cfg, fees)
    assert strategy.desired(market, AccountState(position_qty=-1)) == []
    assert strategy.halt_reason(market, AccountState(position_qty=-1))
    strategy.anchor = 90
    strategy._maybe_recenter(100, AccountState(position_qty=0.0001))
    assert strategy.anchor == 90


def test_explicit_anchor_overrides_saved_anchor_and_never_recenters(tmp_path):
    cfg, market, fees = setup(tmp_path, "grid")
    cfg = replace(cfg, grid=replace(cfg.grid, anchor_price=99))
    strategy = GridStrategy(cfg, fees)
    strategy.load_state({"inventory_direction": "long", "anchor": 100, "blocked": {"1": {}}})
    assert strategy.anchor == 99 and not strategy.blocked
    strategy._maybe_recenter(200, AccountState())
    assert strategy.anchor == 99


@pytest.mark.parametrize("key,value", [("spacing_bps", 0), ("spacing_bps", float("nan")), ("order_notional_usd", float("inf")), ("inventory_direction", "neutral"), ("anchor_price", True), ("spacing_bps", 2000)])
def test_grid_rejects_invalid_config(tmp_path, key, value):
    raw = json.loads((Path(__file__).parents[1] / "config.grid.json").read_text(encoding="utf-8"))
    # 2000 bps is invalid at five levels: the lowest grid price is zero.
    # The user's chosen level count is unrelated to this boundary test.
    raw["grid"]["levels_each_side"] = 5
    raw["grid"][key] = value
    path = tmp_path / "invalid.json"
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(ConfigError):
        load_config(path)


def test_same_account_market_writer_lock(tmp_path):
    first = InstanceLock(tmp_path, "test-account:io:SNDK")
    second = InstanceLock(tmp_path, "test-account:io:SNDK")
    first.acquire()
    try:
        with pytest.raises(RuntimeError, match="已有策略"):
            second.acquire()
    finally:
        first.close()
    second.acquire()
    second.close()


def test_unchanged_strategy_state_skips_disk_rewrites(tmp_path, monkeypatch):
    import entropy_bot.storage as storage
    writes = []
    original = storage.atomic_write_json
    def write(path, payload):
        writes.append(payload)
        original(path, payload)
    monkeypatch.setattr(storage, "atomic_write_json", write)
    ledger = Ledger(tmp_path, "paper", "grid", "io:SNDK", "test")
    state = {"blocked": {}}
    ledger.save_state(state, {})
    for _ in range(100):
        ledger.save_state(state, {})
    assert len(writes) == 1
    state["blocked"]["1"] = {"price": 99}
    ledger.save_state(state, {})
    assert len(writes) == 2


def test_grid_cancel_does_not_place_using_pre_cancel_inventory(tmp_path):
    cfg, market, fees = setup(tmp_path, "grid")
    engine = Engine(cfg)
    broker = PaperBroker(cfg, market.meta, fees)
    engine.broker, engine.snapshot, engine.fees = broker, market, fees
    engine.strategy = GridStrategy(cfg, fees)
    engine.ledger = Ledger(tmp_path, "paper", "grid", "io:SNDK", "test")
    broker.orders_by_role["stale"] = TrackedOrder("stale", "x", True, 99, 1, False, "Alo", 0)
    class Runtime:
        async def audit(self, **kwargs):
            return True
        def snapshot(self, *args):
            return {"inventory_qty": 0}
        def remember(self, *args):
            pass
    engine.grid_runtime = Runtime()
    async def cancel(roles):
        broker.orders_by_role.clear()
        broker.account.position_qty = 1  # a fill arrives during cancellation
    broker.cancel_roles = cancel
    placed = []
    async def place(items, book):
        placed.extend(items)
    broker.place = place
    asyncio.run(engine._reconcile(force=True))
    assert not placed


def test_grid_never_ignores_small_downsize_at_inventory_cap(tmp_path):
    cfg, market, fees = setup(tmp_path, "grid")
    engine = Engine(cfg)
    engine.snapshot = market
    old = TrackedOrder("grid:1", "x", False, 100.1, 1, True, "Alo", 0)
    assert engine._needs_replace(old, OrderIntent("grid:1", False, 100.1, 0.999, True))


def test_pre_crash_submission_unknown_does_not_allow_restart(tmp_path):
    from test_grid_runtime import live_runtime, run
    runtime, market, _ = live_runtime(tmp_path)
    run(runtime.initialize())
    runtime.ledger.record_order_intent({"event": "place", "role": "grid:-1", "cloid": "lost", "time_ms": 1500, "outcome": "submitted"})
    runtime.close()
    restored, _, _ = live_runtime(tmp_path, market)
    try:
        run(restored.initialize())
        assert not restored.ready()
        assert "未决下单意图" in restored.snapshot()["reason"]
        assert restored.book.snapshot()["fill_count"] == 0
    finally:
        restored.close()


def test_pre_crash_fill_recovers_original_intent_once(tmp_path):
    from test_grid_runtime import live_runtime, run, position, raw_fill
    runtime, market, _ = live_runtime(tmp_path)
    run(runtime.initialize())
    runtime.ledger.record_order_intent({"event": "place", "role": "grid:-1", "cloid": "lost", "time_ms": 1500, "outcome": "submitted"})
    runtime.close()
    market.statuses["lost"] = {"status": "order", "order": {"status": "filled", "order": {"coin": "io:TEST", "cloid": "lost", "oid": 77, "origSz": "1", "sz": "0"}}}
    market.fills = [raw_fill("confirmed", oid=77, cloid="", px=100)]
    market.account = position(1, 100, timestamp=2000)
    restored, _, _ = live_runtime(tmp_path, market)
    try:
        run(restored.initialize())
        assert restored.ready(), restored.snapshot()["reason"]
        assert restored.snapshot()["unmatched_qty"] == 1
        assert restored.snapshot()["fill_count"] == 1
    finally:
        restored.close()
    market.calls.clear()
    again, _, _ = live_runtime(tmp_path, market)
    try:
        run(again.initialize())
        assert again.ready()
        assert not any(c[0] == "order_status" and c[2] == "lost" for c in market.calls)
        assert again.snapshot()["fill_count"] == 1
    finally:
        again.close()
