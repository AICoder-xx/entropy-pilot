"""Offline regressions for the 2026-10-17 core audit. Never creates a signer."""

import asyncio
import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from entropy_bot.broker import LiveBroker, PendingFillReconciliation
from entropy_bot.config import load_config
from entropy_bot.engine import Engine
from entropy_bot.fees import build_fee_model
from entropy_bot.models import AccountState, Fill, OrderIntent
from entropy_bot.storage import Ledger, order_intent_is_pending
from entropy_bot.strategies import _allocate_intents
from test_grid_runtime import COIN, ReadOnlyMarket, live_runtime
from test_ws_and_modify import _routing_engine, objects, tracked


def official_status(cloid="pending", coin="io:SNDK", state="canceled", oid=7):
    return {"status": "order", "order": {"status": state, "order": {
        "cloid": cloid, "coin": coin, "oid": oid, "origSz": "1", "sz": "1",
    }}}


@pytest.mark.parametrize("phase", ["cancel", "modify"])
@pytest.mark.parametrize("net_position_changed", [True, False])
def test_maker_reallocates_after_fill_during_mutation(phase, net_position_changed):
    engine, broker = _routing_engine()
    role = "mm:bid:0"
    broker.orders_by_role[role] = tracked(role, "0x" + "1" * 32, is_buy=True, oid=1)
    desired = [OrderIntent("mm:new", True, 99, 1)]
    if phase == "modify":
        desired.insert(0, OrderIntent(role, True, 98.5, 1))
    engine.strategy = SimpleNamespace(desired=lambda *_: desired, state_dict=lambda: {})
    engine._record_fills = lambda: True
    original = getattr(broker, phase + ("_roles" if phase == "cancel" else "_orders"))

    async def fill_during_mutation(*args):
        await original(*args)
        broker.account.volume_usd += 100
        if net_position_changed:
            broker.account.position_qty += 1

    setattr(broker, phase + ("_roles" if phase == "cancel" else "_orders"), fill_during_mutation)
    asyncio.run(engine._reconcile(force=True))
    assert not any(kind == "place" for kind, _ in broker.calls)


def test_broker_semantic_modify_does_not_place_stale_allocation():
    cfg, meta, fees, book = objects()
    broker = LiveBroker(cfg, meta, fees, SimpleNamespace(), "unused", "0x" + "1" * 40)
    broker.exchange = SimpleNamespace()
    order = tracked("mm:bid:0", "0x" + "1" * 32, is_buy=True, oid=1)
    broker.orders_by_role[order.role] = order
    placed = []

    async def cancel_with_fill(roles):
        broker.orders_by_role.clear()
        broker.account.position_qty += 1

    async def capture_place(*args):
        placed.append(args)

    broker.cancel_roles = cancel_with_fill
    broker.place = capture_place
    asyncio.run(broker.modify_orders([(order.role, OrderIntent(order.role, True, 99, 1, True))], book))
    assert placed == []


@pytest.mark.parametrize("outcome", [
    "submitted", "send_uncertain", "unknown", "unknown_response",
    "malformed_top_response", "malformed_response", "contradictory_response",
    "abnormal_resting", "filled_without_oid", "reconciled_order", "reconciled_unknown",
])
def test_malformed_ioc_outcomes_remain_pending_across_restart(tmp_path, outcome):
    ledger = Ledger(tmp_path, "live", "market_maker", "io:SNDK", "old-run")
    ledger.record_order_intent({"event": "inventory_ioc", "cloid": "pending", "outcome": outcome})
    restarted = Ledger(tmp_path, "live", "grid", "io:SNDK", "new-run")
    assert [row["cloid"] for _, row in restarted.pending_order_intents()] == ["pending"]
    assert order_intent_is_pending({"outcome": outcome})


def test_gui_strategy_directories_share_unknown_gate_without_crossing_wallets(tmp_path):
    wallet_root = tmp_path / "wallets" / "selected"
    mm = Ledger(wallet_root, "live", "market_maker", "io:SNDK", "mm-run")
    grid = Ledger(wallet_root / "strategies" / "grid", "live", "grid", "io:SNDK", "grid-run")
    foreign = Ledger(tmp_path / "wallets" / "other", "live", "market_maker", "io:SNDK", "foreign-run")
    for ledger, cloid in [(mm, "mm-pending"), (grid, "grid-pending"), (foreign, "foreign")]:
        ledger.record_order_intent({"event": "place", "cloid": cloid, "outcome": "submitted"})
    for ledger in (mm, grid):
        assert {row["cloid"] for _path, row in ledger.pending_order_intents()} == {"mm-pending", "grid-pending"}


@pytest.mark.parametrize("outcome", ["resting", "filled", "reconciled_open", "recovery_confirmed"])
def test_nominal_success_without_oid_cannot_release_restart_gate(tmp_path, outcome):
    ledger = Ledger(tmp_path, "live", "market_maker", "io:SNDK", "run")
    ledger.record_order_intent({"event": "place", "cloid": "pending", "outcome": "submitted"})
    ledger.record_order_events([{"event": "place", "cloid": "pending", "outcome": outcome, "oid": None}])
    assert len(ledger.pending_order_intents()) == 1
    ledger.record_order_events([{"event": "place", "cloid": "pending", "outcome": outcome, "oid": 7}])
    assert ledger.pending_order_intents() == []


@pytest.mark.parametrize("bad_oid", [None, True, -7, 1.5, "1.5", ""])
@pytest.mark.parametrize("operation", ["place", "modify"])
def test_malformed_resting_response_keeps_broker_and_restart_identity_pending(tmp_path, bad_oid, operation):
    cfg, meta, fees, book = objects()
    broker = LiveBroker(cfg, meta, fees, SimpleNamespace(), "unused", "0x" + "1" * 40)
    broker.exchange = SimpleNamespace(bulk_orders=lambda *_: None, bulk_modify_orders_new=lambda *_: None)
    ledger = Ledger(tmp_path, "live", "market_maker", meta.coin, "run")
    broker.order_intent_sink = ledger.record_order_intent

    async def malformed_sdk(*_):
        return {"status": "ok", "response": {"data": {"statuses": [{"resting": {"oid": bad_oid}}]}}}

    broker._sdk = malformed_sdk
    if operation == "place":
        asyncio.run(broker.place([OrderIntent("mm:bid:0", True, 99, 1)], book))
    else:
        broker.orders_by_role["mm:bid:0"] = tracked("mm:bid:0", "0x" + "1" * 32, is_buy=True, oid=7)
        asyncio.run(broker.modify_orders([("mm:bid:0", OrderIntent("mm:bid:0", True, 98, 1))], book))
    ledger.record_order_events(broker.drain_order_events())
    assert broker.orders_by_role["mm:bid:0"].oid is None
    assert len(ledger.pending_order_intents()) == 1


def recovery_engine(tmp_path, response):
    cfg = replace(load_config(Path(__file__).parents[1] / "config.market_maker.json"), mode="live")
    engine = Engine(cfg)
    engine.ledger = Ledger(tmp_path, "live", "market_maker", "io:SNDK", "new-run")
    engine.ledger.record_order_intent({"event": "place", "cloid": "pending", "outcome": "submitted"})
    calls = []

    async def status(address, cloid):
        calls.append(cloid)
        return response

    engine.market = SimpleNamespace(order_status=status)
    engine.broker = SimpleNamespace(address="test-only", meta=SimpleNamespace(coin="io:SNDK"))
    return engine, calls


@pytest.mark.parametrize("response", [
    {"status": "unknownOid"},
    official_status(cloid="different"),
    official_status(coin="io:ANTH"),
    official_status(oid=None),
    official_status(oid=True),
    official_status(state="unknown"),
])
def test_maker_restart_blocks_unknown_or_mismatched_order(tmp_path, response):
    engine, calls = recovery_engine(tmp_path, response)
    with pytest.raises(RuntimeError, match="UNKNOWN"):
        asyncio.run(engine._recover_order_intents())
    assert calls == ["pending"]
    assert len(engine.ledger.pending_order_intents()) == 1


def test_maker_restart_recovers_positive_identity_and_preserves_original_journal(tmp_path):
    engine, calls = recovery_engine(tmp_path, official_status())
    original = engine.ledger.orders_path.read_bytes()
    asyncio.run(engine._recover_order_intents())
    assert calls == ["pending"]
    assert engine.ledger.pending_order_intents() == []
    assert engine.ledger.orders_path.read_bytes().startswith(original)
    last = json.loads(engine.ledger.orders_path.read_text(encoding="utf-8").splitlines()[-1])
    assert last["outcome"] == "recovery_confirmed"
    assert last["exchange_state"] == "canceled"


def test_corrupt_durable_journal_is_not_silently_treated_as_no_pending_send(tmp_path):
    engine, _calls = recovery_engine(tmp_path, official_status())
    with engine.ledger.orders_path.open("a", encoding="utf-8") as stream:
        stream.write('{"event":"place","cloid":')
    with pytest.raises(RuntimeError, match="UNKNOWN"):
        asyncio.run(engine._recover_order_intents())


@pytest.mark.parametrize("outcome", ["malformed_response", "unknown_response", "filled_without_oid"])
def test_grid_restart_keeps_malformed_intent_pending(tmp_path, outcome):
    def prepare(ledger):
        ledger.record_order_intent({"event": "flatten", "cloid": "pending", "outcome": outcome})

    runtime, _market, _broker = live_runtime(tmp_path, prepare=prepare)
    try:
        asyncio.run(runtime.initialize())
        assert not runtime.ready()
        assert "UNKNOWN" in runtime.reason
    finally:
        runtime.close()


@pytest.mark.parametrize("response", [official_status(cloid="different", coin=COIN), official_status(coin="io:OTHER")])
def test_grid_restart_requires_matching_cloid_and_market(tmp_path, response):
    market = ReadOnlyMarket()
    market.statuses["pending"] = response

    def prepare(ledger):
        ledger.record_order_intent({"event": "place", "cloid": "pending", "outcome": "submitted"})

    runtime, _market, _broker = live_runtime(tmp_path, market, prepare=prepare)
    try:
        asyncio.run(runtime.initialize())
        assert not runtime.ready()
        assert "UNKNOWN" in runtime.reason
    finally:
        runtime.close()


def test_unknown_fill_fee_remains_unknown_in_markout(tmp_path):
    ledger = Ledger(tmp_path, "live", "market_maker", "io:SNDK", "run")
    fill = Fill("f", "mm:bid:0", "c", 1, True, 100, 1, 0, "maker", 1000, fee_known=False)
    row = ledger.record_markout(fill, 30, 99, observed_ms=31000)
    assert row["markout_pnl_usd"] == -1
    assert row["projected_execution_wear_usd"] is None
    assert row["fee_known"] is False


def test_numeric_zero_fee_rates_are_preserved():
    _cfg, meta, _fees, _book = objects()
    fees = build_fee_model(meta, {"userAddRate": 0, "userCrossRate": 0}, self_rebate_pct=0, referred_benefit_pct=0)
    assert fees.gross_maker_rate == fees.gross_taker_rate == 0


@pytest.mark.parametrize("response", [{}, {"userAddRate": "nan", "userCrossRate": "0.0004"}, {"userAddRate": "0", "userCrossRate": "inf"}])
def test_bad_fee_response_cannot_become_confirmed_base_tier(response):
    _cfg, meta, _fees, _book = objects()
    with pytest.raises(ValueError):
        build_fee_model(meta, response, self_rebate_pct=0, referred_benefit_pct=0)


@pytest.mark.parametrize("position,buy,price", [(0, True, 90), (3, True, 90), (0, False, 110), (-3, False, 110)])
def test_maker_inventory_cap_uses_fair_value_not_limit_price(position, buy, price):
    _cfg, meta, _fees, _book = objects()
    orders = _allocate_intents(
        [OrderIntent(f"mm:{i}", buy, price + i, 10) for i in range(3)],
        account=AccountState(position_qty=position), fair=100,
        max_position_usd=1000, entry_limit_usd=500,
        max_open_order_usd=5000, min_notional=1, qty_floor=meta.qty_floor,
    )
    worst = position + sum(o.qty if o.is_buy else -o.qty for o in orders)
    assert abs(worst * 100) <= 500 + 1e-8


@pytest.mark.parametrize("bad", [{"side": "?"}, {"px": "nan"}, {"sz": "inf"}, {"px": "oops"}, {"tid": None}])
def test_bad_fill_cannot_poison_position_or_dedup_before_corrected_row(bad):
    cfg, meta, fees, _book = objects()
    broker = LiveBroker(cfg, meta, fees, SimpleNamespace(), "unused", "0x" + "1" * 40)
    order = tracked("mm:bid:0", "0x" + "1" * 32, is_buy=True, oid=7)
    broker.orders_by_role[order.role] = order
    broker._oid_to_role[7] = order.role
    row = {"coin": meta.coin, "cloid": order.cloid, "oid": 7, "tid": "fill-1",
           "time": 1000, "side": "B", "px": "99", "sz": "1", "fee": "0.01",
           "feeToken": "USDC", "closedPnl": "0"}
    with pytest.raises(RuntimeError):
        broker._ingest_fill_row({**row, **bad})
    assert broker.account.position_qty == 0
    assert not broker._seen_fills
    broker._ingest_fill_row(row)
    assert broker.account.position_qty == 1
    assert broker.account.volume_usd == 99


@pytest.mark.parametrize("bad", [
    {"marginSummary": {"accountValue": "100"}},
    {"assetPositions": [], "marginSummary": {"accountValue": "nan"}},
    {"assetPositions": [], "marginSummary": {}},
    {"assetPositions": [{"position": {"coin": "io:SNDK", "szi": "nan", "entryPx": "100"}}], "marginSummary": {"accountValue": "100"}},
])
def test_bad_account_snapshot_preserves_previous_inventory_and_equity(bad):
    cfg, meta, fees, _book = objects()
    broker = LiveBroker(cfg, meta, fees, SimpleNamespace(), "unused", "0x" + "1" * 40)
    broker.account.position_qty = 2
    broker.account.entry_px = 100
    broker.account.equity_actual = 800
    original = dict(vars(broker.account))
    with pytest.raises(RuntimeError):
        broker._apply_account_payload({"time": 2000, **bad}, {}, {})
    assert vars(broker.account) == original


def test_position_only_reconciliation_does_not_turn_missing_array_into_zero():
    cfg, meta, fees, _book = objects()

    async def malformed_state(_address):
        return {}

    broker = LiveBroker(cfg, meta, fees, SimpleNamespace(account_state=malformed_state), "unused", "0x" + "1" * 40)
    broker.account.position_qty = 2
    with pytest.raises(RuntimeError, match="assetPositions"):
        asyncio.run(broker._sync_position_only())
    assert broker.account.position_qty == 2


@pytest.mark.parametrize("snapshot_first", [True, False])
def test_account_snapshot_and_fill_same_execution_cannot_double_position(snapshot_first):
    cfg, meta, fees, _book = objects()
    broker = LiveBroker(cfg, meta, fees, SimpleNamespace(), "unused", "0x" + "1" * 40)
    order = tracked("mm:bid:0", "0x" + "1" * 32, is_buy=True, oid=7)
    broker.orders_by_role[order.role] = order
    broker._oid_to_role[7] = order.role
    state = {"time": 1100, "assetPositions": [{"position": {
        "coin": meta.coin, "szi": "1", "entryPx": "99", "unrealizedPnl": "1",
    }}], "marginSummary": {"accountValue": "1000"}}
    row = {"coin": meta.coin, "cloid": order.cloid, "oid": 7, "tid": "fill-1",
           "time": 1000, "side": "B", "px": "99", "sz": "1", "fee": "0.01",
           "feeToken": "USDC", "closedPnl": "0"}
    if snapshot_first:
        broker._apply_account_payload(state, {}, {})
        broker._ingest_fill_row(row)
    else:
        broker._ingest_fill_row(row)
        broker._apply_account_payload(state, {}, {})
    assert broker.account.position_qty == 1
    missing_time = dict(state)
    missing_time.pop("time")
    with pytest.raises(RuntimeError, match="快照时间"):
        broker._apply_account_payload(missing_time, {}, {})
    assert broker.account.position_qty == 1
    assert broker.account.volume_usd == 99
    with pytest.raises(PendingFillReconciliation):
        broker._apply_account_payload({**state, "time": 900, "assetPositions": []}, {}, {})
    assert broker.account.position_qty == 1
