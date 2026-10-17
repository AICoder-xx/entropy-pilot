"""Real SQLite/runtime reconciliation against read-only exchange doubles."""

import asyncio
from copy import deepcopy
from types import SimpleNamespace

import pytest

from entropy_bot.broker import PaperBroker
from entropy_bot.grid_runtime import GridRuntime
from entropy_bot.models import AccountState, FeeModel, MarketMeta, OrderBook, TrackedOrder
from entropy_bot.storage import Ledger


COIN = "io:TEST"


@pytest.fixture(autouse=True)
def fixed_reconciliation_clock(monkeypatch):
    # Paper fills use this local clock. Live query boundaries must instead use
    # the independently advancing official account snapshot clock below.
    monkeypatch.setattr("entropy_bot.grid_runtime.time.time", lambda: 10.0)


def meta():
    return MarketMeta(
        coin=COIN, dex="io", symbol="TEST", sz_decimals=4,
        max_leverage=10, only_isolated=True, growth_mode=False,
        deployer_fee_scale=1, is_delisted=False, dex_index=1,
        universe_index=2, asset_id=110002,
    )


def fees():
    return FeeModel(
        user_maker_rate=0.001, user_taker_rate=0.002,
        active_hl_referral_discount=0, deployer_fee_scale=1,
        deployer_share=0.5, growth_mode=False,
        gross_maker_rate=0.001, gross_taker_rate=0.002,
        entropy_self_rebate_pct=160, entropy_referred_benefit_pct=0,
    )


def config(mode="live", direction="long"):
    return SimpleNamespace(
        mode=mode, grid=SimpleNamespace(inventory_direction=direction),
        paper=SimpleNamespace(initial_equity_usd=1000, touch_fill=False),
    )


def position(qty=0, entry=0, timestamp=1000):
    rows = [] if qty == 0 else [{"position": {"coin": COIN, "szi": str(qty), "entryPx": str(entry)}}]
    return {"assetPositions": rows, "time": timestamp}


def raw_fill(tid="1", *, buy=True, px=100, qty=1, fee=0.1, start=0, timestamp=2000, oid=7, cloid="known", **extra):
    return {
        "coin": COIN, "tid": tid, "oid": oid, "cloid": cloid,
        "side": "B" if buy else "A", "px": str(px), "sz": str(qty),
        "fee": str(fee), "feeToken": "USDC", "closedPnl": "0",
        "startPosition": str(start) if start is not None else None,
        "time": timestamp, "crossed": False, **extra,
    }


class ReadOnlyMarket:
    def __init__(self, account=None):
        self.account = account or position()
        self.fills = []
        self.funding = []
        self.statuses = {}
        self.calls = []
        self.fills_error = None
        self.funding_error = None
        self.account_error = None
        self.auto_advance_snapshot_time = True

    async def account_state(self, address):
        self.calls.append(("account_state", address))
        if self.account_error:
            raise self.account_error
        payload = deepcopy(self.account)
        snapshot_time = payload.get("time")
        if self.auto_advance_snapshot_time and isinstance(snapshot_time, (int, float)) and not isinstance(snapshot_time, bool) and snapshot_time > 0:
            # A published account snapshot cannot predate the executions and
            # funding cashflows represented by the same fake exchange state.
            payload["time"] = max([snapshot_time] + [row["time"] for row in self.fills + self.funding])
        return payload

    async def fills_since(self, address, start_ms, *, end_ms):
        self.calls.append(("fills_since", address, start_ms, end_ms))
        if self.fills_error:
            raise self.fills_error
        # Intentionally preserve caller-controlled arrival order so the runtime
        # must establish same-millisecond execution order from startPosition.
        return deepcopy([row for row in self.fills if start_ms <= row["time"] <= end_ms])

    async def user_funding_since(self, address, start_ms, *, end_ms):
        self.calls.append(("user_funding_since", address, start_ms, end_ms))
        if self.funding_error:
            raise self.funding_error
        return deepcopy([row for row in self.funding if start_ms <= row["time"] <= end_ms])

    async def order_status(self, address, oid):
        self.calls.append(("order_status", address, oid))
        return deepcopy(self.statuses.get(oid, {"status": "unknownOid"}))


def official_order(cloid):
    return {"status": "order", "order": {"order": {"cloid": cloid}}}


def live_runtime(tmp_path, market=None, *, direction="long", prepare=None):
    cfg = config(direction=direction)
    ledger = Ledger(tmp_path, "live", "grid", COIN, "test-session")
    if prepare:
        prepare(ledger)
    broker = SimpleNamespace(
        address="test-account", meta=meta(), orders_by_role={}, account=AccountState(),
    )
    market = market or ReadOnlyMarket()
    runtime = GridRuntime(cfg, ledger, market, broker, "test-account:io:TEST")
    runtime.remember({"role": "grid:1", "cloid": "known"})
    return runtime, market, broker


def run(coro):
    return asyncio.run(coro)


def test_live_initialization_uses_authoritative_snapshot_boundary_without_a_signer(tmp_path):
    market = ReadOnlyMarket(position(2, 100, timestamp=1234))
    runtime, market, _ = live_runtime(tmp_path, market)
    try:
        run(runtime.initialize())
        result = runtime.snapshot(110)
        assert result["inception_time_ms"] == 1234
        assert result["inventory_qty"] == 2
        assert result["average_entry_price"] == 100
        assert result["realized_gross_usd"] == 0
        assert result["total_net_usd"] == 20
        assert runtime.ready()
        assert {call[0] for call in market.calls} <= {"account_state", "fills_since", "user_funding_since"}
        assert next(call[2] for call in market.calls if call[0] == "fills_since") == 1234
    finally:
        runtime.close()


@pytest.mark.parametrize("local_seconds", [10.0, 60.0])
def test_live_query_boundaries_use_official_clock_when_host_is_behind_or_ahead(tmp_path, monkeypatch, local_seconds):
    # Exercise an exchange clock ahead of the local clock, then a host
    # clock ahead of the exchange clock.
    monkeypatch.setattr("entropy_bot.grid_runtime.time.time", lambda: local_seconds)
    market = ReadOnlyMarket(position(timestamp=11070))
    market.auto_advance_snapshot_time = False
    runtime, _, _ = live_runtime(tmp_path, market)
    try:
        run(runtime.initialize())
        result = runtime.snapshot(100)
        assert runtime.ready()
        assert result["inception_time_ms"] == 11070
        assert result["funding_coverage_until_ms"] == 11070
        assert result["total_net_usd"] == 0
        initial_queries = [call for call in market.calls if call[0] in {"fills_since", "user_funding_since"}]
        assert initial_queries and all(call[3] == 11070 for call in initial_queries)

        market.calls.clear()
        market.account = position(1, 100, timestamp=12000)
        market.fills = [
            raw_fill("within-official-window", timestamp=12000),
            raw_fill("future-unconfirmed", start=1, timestamp=13000),
        ]
        market.funding = [
            {"time": 12000, "hash": "current", "delta": {"coin": COIN, "usdc": "-0.5"}},
            {"time": 13000, "hash": "future", "delta": {"coin": COIN, "usdc": "-99"}},
        ]
        assert run(runtime.audit(force=True))
        result = runtime.snapshot(110)
        assert result["fill_count"] == 1
        assert result["inventory_qty"] == 1
        assert result["funding_event_count"] == 1
        assert result["funding_net_usd"] == -0.5
        assert result["funding_coverage_until_ms"] == 12000
        assert result["total_net_usd"] == pytest.approx(9.4)
        assert not runtime.book.has_fill("future-unconfirmed")
        bounded_queries = [call for call in market.calls if call[0] in {"fills_since", "user_funding_since"}]
        assert bounded_queries and all(call[3] == 12000 for call in bounded_queries)
        assert market.calls[0][0] == "account_state"
    finally:
        runtime.close()


def test_official_snapshot_older_than_inception_is_pending_without_journal_changes(tmp_path):
    market = ReadOnlyMarket(position(timestamp=11070))
    market.auto_advance_snapshot_time = False
    runtime, _, _ = live_runtime(tmp_path, market)
    try:
        run(runtime.initialize())
        assert runtime.ready()
        before = runtime.book.snapshot(100)
        market.calls.clear()
        market.account = position(timestamp=11000)
        assert not run(runtime.audit(force=True))
        result = runtime.snapshot(100)
        assert not runtime.ready()
        assert result["status"] == "unknown"
        assert result["total_net_usd"] is None
        after = runtime.book.snapshot(100)
        for key in ("fill_count", "inventory_qty", "funding_event_count", "inception_time_ms", "funding_coverage_until_ms"):
            assert after[key] == before[key]
        assert not any(call[0] in {"fills_since", "user_funding_since"} for call in market.calls)
    finally:
        runtime.close()


@pytest.mark.parametrize("timestamp", [None, True, 0, -1, "1000"])
def test_live_bootstrap_rejects_missing_or_invalid_official_snapshot_time(tmp_path, timestamp):
    runtime, _, _ = live_runtime(tmp_path, ReadOnlyMarket(position(timestamp=timestamp)))
    try:
        with pytest.raises(ValueError, match="时间边界"):
            run(runtime.initialize())
        assert runtime.book.snapshot()["initialized"] is False
    finally:
        runtime.close()


def test_live_fee_funding_pairing_and_overlapping_rest_audits_are_idempotent(tmp_path):
    runtime, market, _ = live_runtime(tmp_path)
    try:
        run(runtime.initialize())
        market.fills = [
            raw_fill("buy", px=100, fee=0.1),
            raw_fill("sell", buy=False, px=105, fee=0.2, start=1, timestamp=3000),
        ]
        market.funding = [{"time": 2500, "hash": "f1", "delta": {"coin": COIN, "usdc": "-0.5"}}]
        assert run(runtime.audit(force=True))
        result = runtime.snapshot(105)
        assert result["grid_pair_net_usd"] == pytest.approx(4.7)
        assert result["realized_net_usd"] == pytest.approx(4.2)
        assert result["fees_paid_usd"] == pytest.approx(0.3)
        assert result["funding_net_usd"] == -0.5
        assert result["fill_count"] == 2
        assert result["funding_event_count"] == 1
        assert run(runtime.audit(force=True))
        repeated = runtime.snapshot(105)
        for key in ("fill_count", "funding_event_count", "grid_pair_net_usd", "realized_net_usd", "fees_paid_usd"):
            assert repeated[key] == result[key]
    finally:
        runtime.close()


def test_legacy_and_other_market_fills_do_not_modify_bootstrapped_inventory(tmp_path):
    market = ReadOnlyMarket(position(1, 100))
    market.fills = [raw_fill("boundary", timestamp=1000), raw_fill("other", coin="io:OTHER")]
    runtime, _, _ = live_runtime(tmp_path, market)
    try:
        run(runtime.initialize())
        result = runtime.snapshot(100)
        assert result["inventory_qty"] == 1
        assert result["fill_count"] == 0
        assert result["unmatched_qty"] == 0
    finally:
        runtime.close()


def test_unknown_oid_is_resolved_to_durable_cloid_for_grid_attribution(tmp_path):
    def prepare(ledger):
        ledger.record_order_events([{"event": "place_intent", "role": "grid:-1", "cloid": "durable-cloid"}])

    runtime, market, _ = live_runtime(tmp_path, prepare=prepare)
    try:
        run(runtime.initialize())
        market.fills = [raw_fill("buy", oid=77, cloid="", px=90)]
        market.statuses[77] = official_order("durable-cloid")
        market.account = position(1, 90)
        assert run(runtime.audit(force=True))
        result = runtime.snapshot(90)
        assert result["unmatched_qty"] == 1
        assert result["fill_count"] == 1
        assert runtime.roles["77"] == "grid:-1"
        assert [call[2] for call in market.calls if call[0] == "order_status"] == [77]
        assert run(runtime.audit(force=True))
        assert len([call for call in market.calls if call[0] == "order_status"]) == 1
    finally:
        runtime.close()


def test_unknown_order_ownership_stays_pending_without_guessing_external(tmp_path):
    runtime, market, _ = live_runtime(tmp_path)
    try:
        run(runtime.initialize())
        market.fills = [raw_fill("buy", oid=77, cloid="")]
        market.account = position(1, 100)
        assert not run(runtime.audit(force=True))
        assert runtime.book.snapshot()["fill_count"] == 0
        assert runtime.snapshot(100)["status"] == "unknown"
        assert not runtime.ready()
        market.statuses[77] = official_order("known")
        assert run(runtime.audit(force=True))
        assert runtime.snapshot(100)["unmatched_qty"] == 1
    finally:
        runtime.close()


def test_confirmed_external_fill_changes_cost_but_not_grid_pair_inventory(tmp_path):
    runtime, market, _ = live_runtime(tmp_path)
    try:
        run(runtime.initialize())
        market.fills = [raw_fill("buy", oid=77, cloid="", px=90)]
        market.statuses[77] = official_order("foreign-cloid")
        market.account = position(1, 90)
        assert run(runtime.audit(force=True))
        result = runtime.snapshot(95)
        assert result["inventory_qty"] == 1
        assert result["average_entry_price"] == 90
        assert result["unmatched_qty"] == 0
        assert result["grid_pair_net_usd"] == 0
        assert result["total_net_usd"] == pytest.approx(4.9)
    finally:
        runtime.close()


@pytest.mark.parametrize("missing_start", [None, 2])
def test_incomplete_start_position_chain_only_pending_and_can_recover(tmp_path, missing_start):
    runtime, market, _ = live_runtime(tmp_path)
    try:
        run(runtime.initialize())
        market.fills = [raw_fill("buy", start=missing_start)]
        market.account = position(1, 100)
        assert not run(runtime.audit(force=True))
        state = runtime.book.snapshot(100)
        assert state["status"] == "ready"
        assert state["fill_count"] == 0
        assert state["inventory_qty"] == 0
        assert "仓位链" in runtime.snapshot(100)["reason"]
        market.fills[0]["startPosition"] = "0"
        assert run(runtime.audit(force=True))
        assert runtime.snapshot(100)["fill_count"] == 1
    finally:
        runtime.close()


def test_later_gap_in_batch_does_not_partially_commit_earlier_valid_fill(tmp_path):
    runtime, market, _ = live_runtime(tmp_path)
    try:
        run(runtime.initialize())
        market.fills = [raw_fill("first"), raw_fill("second", start=3, timestamp=3000)]
        market.account = position(4, 100)
        assert not run(runtime.audit(force=True))
        assert runtime.book.snapshot()["fill_count"] == 0
        assert not runtime.book.has_fill("first")
    finally:
        runtime.close()


def test_same_millisecond_out_of_order_rows_are_sorted_by_position_chain(tmp_path):
    runtime, market, _ = live_runtime(tmp_path)
    try:
        run(runtime.initialize())
        market.fills = [
            raw_fill("sell", buy=False, px=105, start=1, timestamp=2000),
            raw_fill("buy", px=100, start=0, timestamp=2000),
        ]
        assert run(runtime.audit(force=True))
        result = runtime.snapshot(105)
        assert result["fill_count"] == 2
        assert result["inventory_qty"] == 0
        assert result["grid_pair_net_usd"] == pytest.approx(4.8)
        assert result["realized_net_usd"] == pytest.approx(4.8)
    finally:
        runtime.close()


def test_same_millisecond_ambiguous_start_position_does_not_guess_order(tmp_path):
    runtime, market, _ = live_runtime(tmp_path)
    try:
        run(runtime.initialize())
        market.fills = [raw_fill("one", px=100), raw_fill("two", px=101)]
        assert not run(runtime.audit(force=True))
        assert runtime.book.snapshot()["fill_count"] == 0
        assert runtime.snapshot(100)["total_net_usd"] is None
    finally:
        runtime.close()


def test_missing_funding_history_keeps_trade_net_but_never_reports_full_net(tmp_path):
    runtime, market, _ = live_runtime(tmp_path)
    try:
        run(runtime.initialize())
        market.fills = [raw_fill("buy", px=100, fee=0.1)]
        market.account = position(1, 100)
        market.funding_error = ConnectionError("funding unavailable")
        assert run(runtime.audit(force=True))
        result = runtime.snapshot(110)
        assert result["funding_complete"] is False
        assert result["realized_after_trade_fees_usd"] == -0.1
        assert result["total_after_trade_fees_usd"] == pytest.approx(9.9)
        for key in ("realized_net_usd", "total_net_usd", "break_even_price", "net_invested_usd"):
            assert result[key] is None
        market.funding_error = None
        assert run(runtime.audit(force=True))
        assert runtime.snapshot(110)["total_net_usd"] == pytest.approx(9.9)
    finally:
        runtime.close()


@pytest.mark.parametrize("failure", ["fills_error", "account_error"])
def test_network_error_hides_stale_complete_pnl_and_preserves_existing_book(tmp_path, failure):
    runtime, market, _ = live_runtime(tmp_path, ReadOnlyMarket(position(1, 100)))
    try:
        run(runtime.initialize())
        assert runtime.snapshot(110)["total_net_usd"] == 10
        setattr(market, failure, ConnectionError("read endpoint unavailable"))
        assert not run(runtime.audit(force=True))
        result = runtime.snapshot(110)
        assert result["status"] == "unknown"
        assert result["net_profit_complete"] is False
        assert not runtime.ready()
        for key in ("realized_net_usd", "total_net_usd", "break_even_price", "grid_pair_net_usd", "total_after_trade_fees_usd"):
            assert result[key] is None
        assert runtime.book.snapshot()["inventory_qty"] == 1
        setattr(market, failure, None)
        assert run(runtime.audit(force=True))
        assert runtime.snapshot(110)["total_net_usd"] == 10
    finally:
        runtime.close()


def test_live_restart_restores_known_inventory_without_reseeding_from_later_official_position(tmp_path):
    runtime, market, _ = live_runtime(tmp_path)
    run(runtime.initialize())
    market.fills = [raw_fill("first", px=100)]
    market.account = position(1, 100)
    assert run(runtime.audit(force=True))
    runtime.close()
    market.account = position(2, 90, timestamp=5000)
    market.fills.append(raw_fill("second", px=80, start=1, timestamp=3000))
    restored, _, _ = live_runtime(tmp_path, market)
    try:
        run(restored.initialize())
        result = restored.snapshot(90)
        assert result["inception_time_ms"] == 1000
        assert result["inventory_qty"] == 2
        assert result["average_entry_price"] == 90
        assert result["fill_count"] == 2
        assert result["fees_paid_usd"] == pytest.approx(0.2)
        assert result["unmatched_qty"] == 2
    finally:
        restored.close()


def test_paper_initialization_and_real_broker_fills_survive_nonflat_restart(tmp_path):
    cfg = config("paper")
    ledger = Ledger(tmp_path, "paper", "grid", COIN, "session-a")
    broker = PaperBroker(cfg, meta(), fees())
    runtime = GridRuntime(cfg, ledger, None, broker, "paper:io:TEST")
    run(runtime.initialize())
    first = TrackedOrder("grid:1", broker.cloids.next(), True, 100, 2, False, "Alo", 0, oid=1)
    broker.orders_by_role[first.role] = first
    broker._fill(first, 100)
    first_fill = broker.drain_fills()[0]
    runtime.record_paper_fill(first_fill)
    assert run(runtime.audit(force=True))
    assert runtime.snapshot(100)["total_net_usd"] == -0.2
    runtime.close()

    restored_broker = PaperBroker(cfg, meta(), fees())
    restored = GridRuntime(cfg, Ledger(tmp_path, "paper", "grid", COIN, "session-b"), None, restored_broker, "paper:io:TEST")
    try:
        run(restored.initialize())
        assert restored_broker.account.position_qty == 2
        assert restored_broker.account.entry_px == 100
        assert restored_broker._initial_equity == pytest.approx(999.8)
        second = TrackedOrder("grid:2", restored_broker.cloids.next(), False, 110, 1, True, "Alo", 0, oid=2)
        restored_broker.orders_by_role[second.role] = second
        restored_broker._fill(second, 110)
        second_fill = restored_broker.drain_fills()[0]
        assert second_fill.fill_id != first_fill.fill_id
        restored.record_paper_fill(second_fill)
        book = OrderBook()
        book.apply_hl([[{"px": "109.99", "sz": "10"}], [{"px": "110.01", "sz": "10"}]])
        run(restored_broker.sync(book))
        assert run(restored.audit(force=True))
        result = restored.snapshot(110)
        assert result["inventory_qty"] == 1
        assert result["fill_count"] == 2
        assert result["grid_pair_net_usd"] == pytest.approx(9.79)
        assert result["realized_net_usd"] == pytest.approx(9.69)
        assert result["total_net_usd"] == pytest.approx(19.69)
        assert restored_broker.account.equity_actual == pytest.approx(1019.69)
    finally:
        restored.close()


def test_short_runtime_uses_signed_start_position_chain(tmp_path):
    runtime, market, _ = live_runtime(tmp_path, direction="short")
    try:
        run(runtime.initialize())
        market.fills = [
            raw_fill("open", buy=False, px=110),
            raw_fill("close", buy=True, px=100, start=-1, timestamp=3000),
        ]
        assert run(runtime.audit(force=True))
        result = runtime.snapshot(100)
        assert result["direction"] == "short"
        assert result["realized_net_usd"] == pytest.approx(9.8)
        assert result["grid_pair_net_usd"] == pytest.approx(9.8)
    finally:
        runtime.close()
