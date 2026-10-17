"""Websocket push-state, batchModify adoption and action-budget behavior."""

import asyncio
import time

import pytest

from entropy_bot.broker import LiveBroker, PaperBroker
from entropy_bot.config import load_config
from entropy_bot.fees import build_fee_model
from entropy_bot.models import MarketMeta, OrderBook, OrderIntent, TrackedOrder
from entropy_bot.pacing import ActionBudget


def objects():
    from pathlib import Path

    cfg = load_config(Path(__file__).parents[1] / "config.market_maker.json")
    meta = MarketMeta(
        coin="io:SNDK",
        dex="io",
        symbol="SNDK",
        sz_decimals=4,
        max_leverage=10,
        only_isolated=True,
        growth_mode=True,
        deployer_fee_scale=1,
        is_delisted=False,
        dex_index=1,
        universe_index=2,
        asset_id=110002,
    )
    fees = build_fee_model(meta, None, self_rebate_pct=160, referred_benefit_pct=0)
    book = OrderBook()
    book.apply_hl(
        [[{"px": "99.9", "sz": "10"}], [{"px": "100.1", "sz": "10"}]]
    )
    return cfg, meta, fees, book


class FakeWsMarket:
    """Market stub whose websocket connection reports alive."""

    ws_session = 1

    def ws_alive_within(self, _max_age):
        return True


def make_live_broker(cfg, meta, fees, market=None) -> LiveBroker:
    return LiveBroker(
        cfg,
        meta,
        fees,
        market if market is not None else FakeWsMarket(),  # type: ignore[arg-type]
        "not-used-in-this-test",
        "0x" + "1" * 40,
    )


def tracked(role: str, cloid: str, *, is_buy: bool, oid: int | None) -> TrackedOrder:
    return TrackedOrder(
        role=role,
        cloid=cloid,
        is_buy=is_buy,
        price=99.0 if is_buy else 101.0,
        qty=1.0,
        reduce_only=False,
        tif="Alo",
        created_ts=time.time(),
        oid=oid,
    )


# ---------------------------------------------------------------------------
# ActionBudget
# ---------------------------------------------------------------------------


def test_action_budget_states_and_local_spend():
    budget = ActionBudget(soft_remaining=2500, hard_remaining=800)
    assert budget.state == "unknown"
    budget.apply_official(
        {"cumVlm": "123456.0", "nRequestsUsed": 1000, "nRequestsCap": 11000}
    )
    assert budget.state == "normal"
    assert budget.remaining == 10000
    budget.spend(7600)
    assert budget.state == "soft"
    budget.spend(1700)
    assert budget.state == "hard"
    # An official refresh replaces the local estimate.
    budget.apply_official(
        {"cumVlm": "200000.0", "nRequestsUsed": 2000, "nRequestsCap": 30000}
    )
    assert budget.state == "normal"
    assert budget.local_spent == 0.0
    assert budget.session_spent == 9300.0
    efficiency = budget.snapshot(session_volume_usd=93_000.0, growth_mode=True)
    assert efficiency["volume_per_action_usd"] == 10.0
    assert efficiency["self_funding_ratio"] == 1.0


# ---------------------------------------------------------------------------
# Websocket ingestion
# ---------------------------------------------------------------------------


def test_ws_terminal_order_event_answers_status_without_rest():
    cfg, meta, fees, _book = objects()

    class NoRestMarket(FakeWsMarket):
        async def order_status(self, _address, _cloid):
            raise AssertionError("terminal WS event must answer without REST")

    broker = make_live_broker(cfg, meta, fees, NoRestMarket())
    cloid = broker.cloids.next()
    broker.ingest_ws_event(
        "orderUpdates",
        [
            {
                "order": {
                    "coin": meta.coin,
                    "cloid": cloid,
                    "oid": 31,
                    "origSz": "1.0",
                    "sz": "0.0",
                },
                "status": "filled",
                "statusTimestamp": 1000,
            }
        ],
    )
    status = asyncio.run(broker._order_status_view(cloid))
    assert status is not None and status.get("status") == "order"
    assert (status.get("order") or {}).get("status") == "filled"


def test_ws_open_event_still_falls_back_to_rest():
    cfg, meta, fees, _book = objects()
    calls = []

    class RestMarket(FakeWsMarket):
        async def order_status(self, _address, cloid):
            calls.append(cloid)
            return {"status": "unknownOid"}

    broker = make_live_broker(cfg, meta, fees, RestMarket())
    cloid = broker.cloids.next()
    broker.ingest_ws_event(
        "orderUpdates",
        [
            {
                "order": {"coin": meta.coin, "cloid": cloid, "oid": 32},
                "status": "open",
                "statusTimestamp": 1000,
            }
        ],
    )
    asyncio.run(broker._order_status_view(cloid))
    assert calls == [cloid]


def test_ws_fills_are_deduplicated_against_rest_sweep():
    cfg, meta, fees, _book = objects()

    row = {
        "coin": "io:SNDK",
        "oid": 41,
        "tid": "t-1",
        "side": "B",
        "px": "99.0",
        "sz": "1.0",
        "fee": "0.01",
        "closedPnl": "0",
        "crossed": False,
        "time": 5_000,
    }

    class RestMarket(FakeWsMarket):
        async def fills_since(self, _address, _start_ms):
            return [dict(row)]

    broker = make_live_broker(cfg, meta, fees, RestMarket())
    order = tracked("bid", broker.cloids.next(), is_buy=True, oid=41)
    broker.orders_by_role["bid"] = order
    broker._oid_to_role[41] = "bid"

    broker.ingest_ws_event("userFills", {"isSnapshot": False, "fills": [dict(row)]})
    assert len(broker._fills) == 1
    assert broker.account.position_qty == pytest.approx(1.0)
    # Full fill retires the role and preserves identity for late rows.
    assert "bid" not in broker.orders_by_role
    assert 41 in broker._retired_orders_by_oid

    # The authoritative REST sweep re-reads the same row: no double count.
    asyncio.run(broker._sync_fills())
    assert len(broker._fills) == 1
    assert broker.account.position_qty == pytest.approx(1.0)
    assert broker.account.volume_usd == pytest.approx(99.0)


def test_ws_reset_invalidates_open_order_snapshot():
    cfg, meta, fees, _book = objects()
    broker = make_live_broker(cfg, meta, fees)
    broker.ingest_ws_event("ws_reset", {"session": 1})
    broker.ingest_ws_event(
        "openOrders",
        {"dex": "io", "orders": [{"coin": meta.coin, "cloid": "0xa", "oid": 1}]},
    )
    assert broker._ws_open_view() is not None
    assert broker.ws_healthy()
    # Reconnect bumps the session: the old snapshot must not be trusted.
    broker.ingest_ws_event("ws_reset", {"session": 2})
    assert broker._ws_open_view() is None
    assert not broker.ws_healthy()


def test_ws_clearinghouse_push_refreshes_account_between_rest_syncs():
    cfg, meta, fees, _book = objects()
    broker = make_live_broker(cfg, meta, fees)
    broker.ingest_ws_event("ws_reset", {"session": 1})
    broker.ingest_ws_event(
        "clearinghouseState",
        {
            "dex": "io",
            "user": broker.address,
            "clearinghouseState": {
                "marginSummary": {"accountValue": "1234.5"},
                "withdrawable": "800.0",
                "assetPositions": [],
                "time": int(time.time() * 1000),
            },
        },
    )
    assert broker.account.equity_actual == pytest.approx(1234.5)
    assert broker.account.withdrawable == pytest.approx(800.0)


# ---------------------------------------------------------------------------
# sync() source selection
# ---------------------------------------------------------------------------


def test_sync_uses_push_snapshot_without_rest_when_ws_healthy():
    cfg, meta, fees, book = objects()

    class NoRestMarket(FakeWsMarket):
        async def open_orders(self, _address):
            raise AssertionError("healthy WS steady state must not poll REST")

        async def fills_since(self, _address, _start_ms):
            raise AssertionError("healthy WS steady state must not poll fills")

    broker = make_live_broker(cfg, meta, fees, NoRestMarket())
    broker.ingest_ws_event("ws_reset", {"session": 1})
    cloid = broker.cloids.next()
    broker.orders_by_role["bid"] = tracked("bid", cloid, is_buy=True, oid=51)
    broker.ingest_ws_event(
        "openOrders",
        {
            "dex": "io",
            "orders": [{"coin": meta.coin, "cloid": cloid, "oid": 51}],
        },
    )
    now = time.monotonic()
    broker._last_rest_sweep = now
    broker._last_account_sync = now
    broker._last_fill_sync = 0.0

    asyncio.run(broker.sync(book))
    assert "bid" in broker.orders_by_role


def test_sync_reconciles_ws_canceled_order_without_rest():
    cfg, meta, fees, book = objects()

    class NoRestMarket(FakeWsMarket):
        async def open_orders(self, _address):
            raise AssertionError("WS terminal event should resolve the role")

    broker = make_live_broker(cfg, meta, fees, NoRestMarket())
    broker.ingest_ws_event("ws_reset", {"session": 1})
    cloid = broker.cloids.next()
    broker.orders_by_role["bid"] = tracked("bid", cloid, is_buy=True, oid=52)
    broker.ingest_ws_event(
        "openOrders", {"dex": "io", "orders": []}
    )
    broker.ingest_ws_event(
        "orderUpdates",
        [
            {
                "order": {
                    "coin": meta.coin,
                    "cloid": cloid,
                    "oid": 52,
                    "origSz": "1.0",
                    "sz": "1.0",
                },
                "status": "canceled",
                "statusTimestamp": 2000,
            }
        ],
    )
    now = time.monotonic()
    broker._last_rest_sweep = now
    broker._last_account_sync = now

    asyncio.run(broker.sync(book))
    assert "bid" not in broker.orders_by_role
    assert 52 in broker._retired_orders_by_oid


def test_sync_falls_back_to_rest_when_ws_stale():
    cfg, meta, fees, book = objects()
    open_calls = []

    class DeadWsMarket:
        ws_session = 1

        def ws_alive_within(self, _max_age):
            return False

        async def open_orders(self, _address):
            open_calls.append(1)
            return []

        async def fills_since(self, _address, _start_ms):
            return []

        async def account_state(self, _address):
            return {
                "time": int(time.time() * 1000),
                "marginSummary": {"accountValue": "1000.0"},
                "withdrawable": "1000.0",
                "assetPositions": [],
            }

    broker = make_live_broker(cfg, meta, fees, DeadWsMarket())
    broker.ingest_ws_event("ws_reset", {"session": 1})
    broker.ingest_ws_event("openOrders", {"dex": "io", "orders": []})
    broker._last_fill_sync = 0.0

    asyncio.run(broker.sync(book))
    assert open_calls, "stale WS must fall back to REST open orders"


@pytest.mark.parametrize("invalid_source", ["event", "account"])
def test_invalid_push_forces_immediate_rest_at_monotonic_origin(monkeypatch, invalid_source):
    monkeypatch.setattr(time, "monotonic", lambda: 0.0)
    cfg, meta, fees, book = objects()
    calls = {"fills": 0, "orders": 0, "account": 0}

    class Market(FakeWsMarket):
        async def open_orders(self, _address):
            calls["orders"] += 1
            return []

        async def fills_since(self, _address, _start_ms):
            calls["fills"] += 1
            return []

        async def account_state(self, _address):
            calls["account"] += 1
            return {
                "time": int(time.time() * 1000),
                "marginSummary": {"accountValue": "100.0"},
                "withdrawable": "100.0", "assetPositions": [],
            }

    broker = make_live_broker(cfg, meta, fees, Market())
    # Model a completed REST sweep at the clock origin, followed immediately
    # by malformed push state. The fallback must not wait for system uptime.
    broker._last_fill_sync = broker._last_full_fill_sync = 0.0
    broker._last_rest_sweep = broker._last_account_sync = 0.0
    if invalid_source == "event":
        broker.ingest_ws_event("ws_reset", {"session": "invalid-session"})
    else:
        broker._ws_clearinghouse = {"marginSummary": {"accountValue": "invalid-number"}}
        broker._apply_ws_account()
    asyncio.run(broker.sync(book))
    assert calls == {"fills": 0, "orders": 1, "account": 1}


# ---------------------------------------------------------------------------
# batchModify
# ---------------------------------------------------------------------------


def test_modify_orders_adopts_new_identity_and_retires_old():
    cfg, meta, fees, book = objects()
    broker = make_live_broker(cfg, meta, fees)

    class FakeExchange:
        bulk_modify_orders_new = object()

    broker.exchange = FakeExchange()
    old_cloid = broker.cloids.next()
    broker.orders_by_role["bid"] = tracked("bid", old_cloid, is_buy=True, oid=61)
    broker._oid_to_role[61] = "bid"
    captured = {}

    async def fake_sdk(_fn, requests):
        captured["requests"] = requests
        return {
            "status": "ok",
            "response": {
                "data": {"statuses": [{"resting": {"oid": 62}}]}
            },
        }

    broker._sdk = fake_sdk  # type: ignore[method-assign]
    intent = OrderIntent("bid", True, 99.5, 1.0)
    asyncio.run(broker.modify_orders([("bid", intent)], book))

    assert captured["requests"][0]["oid"] == 61
    replacement = broker.orders_by_role["bid"]
    assert replacement.oid == 62
    assert replacement.price == 99.5
    assert replacement.cloid != old_cloid
    assert broker._oid_to_role[62] == "bid"
    # Old identity is retired for late fills, exactly like a canceled order.
    assert 61 in broker._retired_orders_by_oid
    assert broker._oid_to_role[61] == "bid"
    assert broker.requotes_modified == 1
    actions = broker.drain_order_events()
    assert len(actions) == 1
    assert actions[0]["event"] == "modify"
    assert actions[0]["outcome"] == "resting"
    assert actions[0]["oid"] == 62
    assert broker.action_budget.session_spent == 1


def test_modify_error_with_canceled_old_order_releases_role():
    cfg, meta, fees, book = objects()

    class RestMarket(FakeWsMarket):
        async def order_status(self, _address, _cloid):
            return {
                "status": "order",
                "order": {"status": "canceled", "order": {"origSz": "1", "sz": "1"}},
            }

    broker = make_live_broker(cfg, meta, fees, RestMarket())

    class FakeExchange:
        bulk_modify_orders_new = object()

    broker.exchange = FakeExchange()
    old_cloid = broker.cloids.next()
    broker.orders_by_role["bid"] = tracked("bid", old_cloid, is_buy=True, oid=63)
    broker._oid_to_role[63] = "bid"

    async def fake_sdk(_fn, _requests):
        return {
            "status": "ok",
            "response": {
                "data": {
                    "statuses": [
                        {"error": "Order was never placed, already canceled, or filled."}
                    ]
                }
            },
        }

    broker._sdk = fake_sdk  # type: ignore[method-assign]
    intent = OrderIntent("bid", True, 99.5, 1.0)
    asyncio.run(broker.modify_orders([("bid", intent)], book))

    # Old order is terminal: the role is released so the next reconcile
    # places a fresh order through the normal path.
    assert "bid" not in broker.orders_by_role
    assert 63 in broker._retired_orders_by_oid
    actions = broker.drain_order_events()
    assert len(actions) == 1
    assert actions[0]["event"] == "modify"
    assert actions[0]["outcome"] == "rejected"
    assert broker.action_budget.session_spent == 1


def test_semantic_modify_is_replaced_without_wasting_a_modify_action():
    """Broker defence routes a reduce-only flip straight to cancel+place."""
    cfg, meta, fees, book = objects()

    class RestMarket(FakeWsMarket):
        async def order_status(self, _address, _cloid):
            # Only the modify-failure resolver reads this: the old order is
            # still resting. The later cancel is confirmed via the WS event.
            return {
                "status": "order",
                "order": {"status": "open", "order": {"origSz": "1", "sz": "1"}},
            }

    broker = make_live_broker(cfg, meta, fees, RestMarket())
    broker.ingest_ws_event("ws_reset", {"session": 1})

    class FakeExchange:
        bulk_modify_orders_new = object()
        bulk_cancel_by_cloid = object()
        bulk_orders = object()

    broker.exchange = FakeExchange()
    old_cloid = broker.cloids.next()
    old_order = tracked("bid", old_cloid, is_buy=True, oid=64)
    broker.orders_by_role["bid"] = old_order
    broker._oid_to_role[64] = "bid"
    calls: list[str] = []

    async def fake_sdk(fn, _requests):
        if fn is FakeExchange.bulk_modify_orders_new:
            calls.append("modify")
            return {
                "status": "ok",
                "response": {
                    "data": {
                        "statuses": [
                            {"error": "Attempted to modify to invalid new order"}
                        ]
                    }
                },
            }
        if fn is FakeExchange.bulk_cancel_by_cloid:
            calls.append("cancel")
            broker.ingest_ws_event(
                "orderUpdates",
                [
                    {
                        "order": {
                            "coin": meta.coin,
                            "cloid": old_cloid,
                            "oid": 64,
                            "origSz": "1.0",
                            "sz": "1.0",
                        },
                        "status": "canceled",
                        "statusTimestamp": 3000,
                    }
                ],
            )
            return {"status": "ok"}
        assert fn is FakeExchange.bulk_orders
        calls.append("place")
        return {
            "status": "ok",
            "response": {"data": {"statuses": [{"resting": {"oid": 65}}]}},
        }

    broker._sdk = fake_sdk  # type: ignore[method-assign]
    intent = OrderIntent("bid", True, 99.0, 1.0, reduce_only=True)
    asyncio.run(broker.modify_orders([("bid", intent)], book))

    assert calls == ["cancel", "place"]
    replacement = broker.orders_by_role["bid"]
    assert replacement.oid == 65
    assert replacement.reduce_only is True
    assert replacement.cloid != old_cloid
    assert 64 in broker._retired_orders_by_oid
    assert broker.requotes_replaced == 1


def test_modify_exception_with_unknown_outcome_fails_closed():
    cfg, meta, fees, book = objects()
    old_cloid_holder = {}

    class RestMarket(FakeWsMarket):
        async def order_status(self, _address, cloid):
            if cloid == old_cloid_holder["cloid"]:
                # Old order in a state the resolver does not understand.
                return {
                    "status": "order",
                    "order": {"status": "triggered", "order": {}},
                }
            # New cloid probe: the exchange never saw the replacement.
            return {"status": "unknownOid"}

    broker = make_live_broker(cfg, meta, fees, RestMarket())

    class FakeExchange:
        bulk_modify_orders_new = object()

    broker.exchange = FakeExchange()
    old_cloid = broker.cloids.next()
    old_cloid_holder["cloid"] = old_cloid
    broker.orders_by_role["bid"] = tracked("bid", old_cloid, is_buy=True, oid=65)

    async def fake_sdk(_fn, _requests):
        raise RuntimeError("socket dropped mid-send")

    broker._sdk = fake_sdk  # type: ignore[method-assign]
    intent = OrderIntent("bid", True, 99.5, 1.0)
    with pytest.raises(RuntimeError, match="改单结果不确定"):
        asyncio.run(broker.modify_orders([("bid", intent)], book))
    # Role stays reserved: no duplicate placement is possible.
    assert "bid" in broker.orders_by_role
    actions = broker.drain_order_events()
    assert len(actions) == 1
    assert actions[0]["event"] == "modify"
    assert actions[0]["outcome"] == "send_uncertain"
    assert broker.action_budget.session_spent == 1


def test_modify_http_429_keeps_predecessor_and_enters_pacing():
    cfg, meta, fees, book = objects()
    broker = make_live_broker(cfg, meta, fees)

    class FakeExchange:
        bulk_modify_orders_new = object()

    class RateLimited(Exception):
        status_code = 429

    broker.exchange = FakeExchange()
    old = tracked("bid", broker.cloids.next(), is_buy=True, oid=164)
    broker.orders_by_role["bid"] = old
    broker._oid_to_role[164] = "bid"

    async def rejected_sdk(_fn, _requests):
        raise RateLimited()

    broker._sdk = rejected_sdk  # type: ignore[method-assign]
    asyncio.run(
        broker.modify_orders(
            [("bid", OrderIntent("bid", True, 99.5, 1.0))], book
        )
    )

    assert broker.orders_by_role["bid"] is old
    assert broker.action_rate_limited is True
    events = broker.drain_order_events()
    assert events[0]["outcome"] == "rate_limited"


def test_uncertain_modify_is_released_by_complete_predecessor_fill():
    cfg, meta, fees, _book = objects()
    broker = make_live_broker(cfg, meta, fees)
    old = tracked("bid", broker.cloids.next(), is_buy=True, oid=170)
    broker.orders_by_role["bid"] = old
    broker._oid_to_role[170] = "bid"
    replacement_cloid = broker.cloids.next()
    broker._reserve_uncertain_modify(
        old,
        OrderIntent("bid", True, 99.5, 1.0),
        replacement_cloid,
        {},
    )
    assert broker.orders_by_role["bid"].oid is None

    broker._ingest_fill_row(
        {
            "coin": meta.coin,
            "cloid": old.cloid,
            "oid": 170,
            "tid": "predecessor-complete",
            "side": "B",
            "px": "99.5",
            "sz": "1",
            "fee": "0.01",
            "closedPnl": "0",
            "crossed": False,
            "time": 123456,
        }
    )

    assert "bid" not in broker.orders_by_role
    assert replacement_cloid not in broker._uncertain_modify_predecessors


@pytest.mark.parametrize("old_state", ["open", "canceled"])
def test_modify_two_unknowns_never_uses_old_state_to_release_role(
    monkeypatch, old_state
):
    cfg, meta, fees, book = objects()
    old_cloid_holder = {}

    class RestMarket(FakeWsMarket):
        def __init__(self):
            self.lookups = []

        async def order_status(self, _address, cloid):
            self.lookups.append(cloid)
            if cloid == old_cloid_holder["cloid"]:
                return {
                    "status": "order",
                    "order": {
                        "status": old_state,
                        "oid": 165,
                        "order": {"oid": 165, "cloid": cloid},
                    },
                }
            return {"status": "unknownOid"}

    market = RestMarket()
    broker = make_live_broker(cfg, meta, fees, market)

    class FakeExchange:
        bulk_modify_orders_new = object()

    broker.exchange = FakeExchange()
    old_cloid = broker.cloids.next()
    old_cloid_holder["cloid"] = old_cloid
    broker.orders_by_role["bid"] = tracked("bid", old_cloid, is_buy=True, oid=165)
    broker._oid_to_role[165] = "bid"
    sdk_calls = 0

    async def dying_sdk(_fn, _requests):
        nonlocal sdk_calls
        sdk_calls += 1
        raise RuntimeError("connection dropped mid-send")

    async def no_sleep(_seconds):
        return None

    broker._sdk = dying_sdk  # type: ignore[method-assign]
    monkeypatch.setattr("entropy_bot.broker.asyncio.sleep", no_sleep)
    intent = OrderIntent("bid", True, 99.5, 1.0)
    with pytest.raises(RuntimeError, match="改单结果不确定"):
        asyncio.run(broker.modify_orders([("bid", intent)], book))

    replacement = broker.orders_by_role["bid"]
    assert replacement.cloid != old_cloid
    assert replacement.oid is None
    assert old_cloid not in market.lookups
    assert 165 in broker._retired_orders_by_oid

    # The oid-less replacement placeholder makes a later modify a no-op; the
    # signed action is never submitted twice.
    asyncio.run(broker.modify_orders([("bid", intent)], book))
    assert sdk_calls == 1


def test_uncertain_modify_late_open_binds_replacement_identity(monkeypatch):
    cfg, meta, fees, book = objects()

    class RestMarket(FakeWsMarket):
        async def order_status(self, _address, _cloid):
            return {"status": "unknownOid"}

    broker = make_live_broker(cfg, meta, fees, RestMarket())

    class FakeExchange:
        bulk_modify_orders_new = object()

    broker.exchange = FakeExchange()
    old_cloid = broker.cloids.next()
    broker.orders_by_role["bid"] = tracked("bid", old_cloid, is_buy=True, oid=166)
    broker._oid_to_role[166] = "bid"
    sdk_calls = 0

    async def dying_sdk(_fn, _requests):
        nonlocal sdk_calls
        sdk_calls += 1
        raise RuntimeError("connection dropped mid-send")

    async def no_sleep(_seconds):
        return None

    broker._sdk = dying_sdk  # type: ignore[method-assign]
    monkeypatch.setattr("entropy_bot.broker.asyncio.sleep", no_sleep)
    intent = OrderIntent("bid", True, 99.5, 1.0)
    with pytest.raises(RuntimeError, match="改单结果不确定"):
        asyncio.run(broker.modify_orders([("bid", intent)], book))
    replacement = broker.orders_by_role["bid"]

    broker._classify_official_orders(
        [{"coin": meta.coin, "cloid": replacement.cloid, "oid": 167}]
    )

    assert broker.orders_by_role["bid"] is replacement
    assert replacement.oid == 167
    assert broker._oid_to_role[167] == "bid"
    assert 166 in broker._retired_orders_by_oid
    assert sdk_calls == 1


def test_modify_exception_with_unreachable_official_view_propagates():
    cfg, meta, fees, book = objects()

    class RestMarket(FakeWsMarket):
        async def order_status(self, _address, _cloid):
            raise RuntimeError("official view unavailable")

    broker = make_live_broker(cfg, meta, fees, RestMarket())

    class FakeExchange:
        bulk_modify_orders_new = object()

    broker.exchange = FakeExchange()
    broker.orders_by_role["bid"] = tracked(
        "bid", broker.cloids.next(), is_buy=True, oid=66
    )

    async def fake_sdk(_fn, _requests):
        raise RuntimeError("socket dropped mid-send")

    broker._sdk = fake_sdk  # type: ignore[method-assign]
    intent = OrderIntent("bid", True, 99.5, 1.0)
    with pytest.raises(RuntimeError):
        asyncio.run(broker.modify_orders([("bid", intent)], book))
    # Fail closed with the role reserved; a stray resting replacement is in
    # the bot cloid namespace and is swept by orphan cancellation later.
    assert "bid" in broker.orders_by_role


def test_modify_exception_waits_for_new_cloid_visibility(monkeypatch):
    """One transient unknownOid must not release a canceled old role."""
    cfg, meta, fees, book = objects()
    old_cloid_holder = {}
    new_calls = []

    class RestMarket(FakeWsMarket):
        async def order_status(self, _address, cloid):
            if cloid == old_cloid_holder["cloid"]:
                return {
                    "status": "order",
                    "order": {
                        "status": "canceled",
                        "order": {"origSz": "1", "sz": "1"},
                    },
                }
            new_calls.append(cloid)
            if len(new_calls) == 1:
                return {"status": "unknownOid"}
            return {
                "status": "order",
                "order": {
                    "status": "open",
                    "oid": 67,
                    "order": {"oid": 67, "origSz": "1", "sz": "1"},
                },
            }

    broker = make_live_broker(cfg, meta, fees, RestMarket())

    class FakeExchange:
        bulk_modify_orders_new = object()

    broker.exchange = FakeExchange()
    old_cloid = broker.cloids.next()
    old_cloid_holder["cloid"] = old_cloid
    broker.orders_by_role["bid"] = tracked("bid", old_cloid, is_buy=True, oid=66)
    broker._oid_to_role[66] = "bid"

    async def fake_sdk(_fn, _requests):
        raise RuntimeError("socket dropped mid-send")

    async def no_sleep(_seconds):
        return None

    broker._sdk = fake_sdk  # type: ignore[method-assign]
    monkeypatch.setattr("entropy_bot.broker.asyncio.sleep", no_sleep)
    asyncio.run(
        broker.modify_orders(
            [("bid", OrderIntent("bid", True, 99.5, 1.0))], book
        )
    )

    replacement = broker.orders_by_role["bid"]
    assert len(new_calls) == 2
    assert replacement.cloid == new_calls[-1]
    assert replacement.cloid != old_cloid
    assert replacement.oid == 67
    assert 66 in broker._retired_orders_by_oid


def test_modify_malformed_response_reconciles_new_identity(monkeypatch):
    cfg, meta, fees, book = objects()

    class RestMarket(FakeWsMarket):
        async def order_status(self, _address, _cloid):
            return {
                "status": "order",
                "order": {
                    "status": "open",
                    "oid": 69,
                    "order": {"oid": 69, "origSz": "1", "sz": "1"},
                },
            }

    broker = make_live_broker(cfg, meta, fees, RestMarket())

    class FakeExchange:
        bulk_modify_orders_new = object()

    broker.exchange = FakeExchange()
    old_cloid = broker.cloids.next()
    broker.orders_by_role["bid"] = tracked("bid", old_cloid, is_buy=True, oid=68)
    broker._oid_to_role[68] = "bid"

    async def fake_sdk(_fn, _requests):
        return {"status": "ok", "response": {"data": {"statuses": []}}}

    broker._sdk = fake_sdk  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="响应数量不匹配"):
        asyncio.run(
            broker.modify_orders(
                [("bid", OrderIntent("bid", True, 99.5, 1.0))], book
            )
        )
    assert broker.orders_by_role["bid"].oid == 69
    assert broker.orders_by_role["bid"].cloid != old_cloid
    assert 68 in broker._retired_orders_by_oid
    actions = broker.drain_order_events()
    assert len(actions) == 1
    assert actions[0]["event"] == "modify"
    assert actions[0]["outcome"] == "reconciled_open"
    assert broker.action_budget.session_spent == 1


def test_paper_modify_orders_fallback_cancels_and_replaces():
    cfg, meta, fees, book = objects()
    broker = PaperBroker(cfg, meta, fees)
    asyncio.run(broker.place([OrderIntent("bid", True, 99.0, 1.0)], book))
    old = broker.orders_by_role["bid"]
    asyncio.run(
        broker.modify_orders([("bid", OrderIntent("bid", True, 99.5, 1.0))], book)
    )
    replacement = broker.orders_by_role["bid"]
    assert replacement.price == 99.5
    assert replacement.cloid != old.cloid
    assert broker.requotes_modified == 1


# ---------------------------------------------------------------------------
# Cancel confirmation via WS
# ---------------------------------------------------------------------------


def test_cancel_confirmed_by_ws_terminal_event_skips_rest_read():
    cfg, meta, fees, _book = objects()

    class NoRestMarket(FakeWsMarket):
        async def open_orders(self, _address):
            raise AssertionError("WS-confirmed cancel must not read open orders")

        async def order_status(self, _address, _cloid):
            return {
                "status": "order",
                "order": {"status": "canceled", "order": {"origSz": "1", "sz": "1"}},
            }

    broker = make_live_broker(cfg, meta, fees, NoRestMarket())
    broker.ingest_ws_event("ws_reset", {"session": 1})

    class FakeExchange:
        bulk_cancel_by_cloid = object()

    broker.exchange = FakeExchange()
    cloid = broker.cloids.next()
    broker.orders_by_role["bid"] = tracked("bid", cloid, is_buy=True, oid=71)

    async def fake_sdk(_fn, _reqs):
        # The cancel ack and the push event arrive around the same time.
        broker.ingest_ws_event(
            "orderUpdates",
            [
                {
                    "order": {
                        "coin": meta.coin,
                        "cloid": cloid,
                        "oid": 71,
                        "origSz": "1.0",
                        "sz": "1.0",
                    },
                    "status": "canceled",
                    "statusTimestamp": 3000,
                }
            ],
        )
        return {"status": "ok"}

    broker._sdk = fake_sdk  # type: ignore[method-assign]
    asyncio.run(broker.cancel_roles(["bid"]))
    assert broker.orders_by_role == {}
    assert 71 in broker._retired_orders_by_oid


# ---------------------------------------------------------------------------
# Reduce-only clipped remainder release
# ---------------------------------------------------------------------------


def _clipped_reduce_only_ask(broker, *, oid: int) -> TrackedOrder:
    cloid = broker.cloids.next()
    order = TrackedOrder(
        role="mm:ask:0",
        cloid=cloid,
        is_buy=False,
        price=101.0,
        qty=0.779,
        reduce_only=True,
        tif="Alo",
        created_ts=time.time(),
        oid=oid,
    )
    order.filled_qty = 0.241
    broker.orders_by_role["mm:ask:0"] = order
    broker._oid_to_role[oid] = "mm:ask:0"
    # Simulate the wait having been running for a while already.
    broker._missing_since[cloid] = time.monotonic() - 30.0
    return order


class _PhantomFilledMarket(FakeWsMarket):
    """orderStatus claims a full fill; clearinghouse tells the truth."""

    def __init__(self, position_szi: str | None):
        self.position_szi = position_szi
        self.account_calls = 0

    async def open_orders(self, _address):
        return []

    async def order_status(self, _address, _cloid):
        # origSz - sz arithmetic wrongly claims 0.779 traded even though the
        # reduce-only remainder was clipped after 0.241.
        return {
            "status": "order",
            "order": {"status": "filled", "order": {"origSz": "0.779", "sz": "0"}},
        }

    async def account_state(self, _address):
        self.account_calls += 1
        positions = []
        if self.position_szi is not None:
            positions.append(
                {
                    "position": {
                        "coin": "io:SNDK",
                        "szi": self.position_szi,
                        "entryPx": "100.0",
                        "unrealizedPnl": "0.0",
                    }
                }
            )
        return {
            "time": int(time.time() * 1000),
            "marginSummary": {"accountValue": "6000.0"},
            "withdrawable": "6000.0",
            "assetPositions": positions,
        }


def test_reduce_only_clip_releases_role_when_nothing_left_to_reduce():
    """A reduce-only order reported 'filled' beyond its true fill must release
    once the clearinghouse shows nothing left to reduce, instead of waiting
    300 s for details of a remainder that never traded and halting."""
    cfg, meta, fees, _book = objects()
    market = _PhantomFilledMarket(position_szi=None)
    broker = make_live_broker(cfg, meta, fees, market)
    order = _clipped_reduce_only_ask(broker, oid=91)

    asyncio.run(broker._reconcile_missing_tracked(set(), set()))

    assert "mm:ask:0" not in broker.orders_by_role
    assert market.account_calls == 1
    # Late userFills rows must still be attributable for ledger accuracy.
    assert 91 in broker._retired_orders_by_oid
    assert broker._oid_to_role[91] == "mm:ask:0"
    del order


def test_reduce_only_wait_persists_while_position_remains_reducible():
    """With inventory still on the reduce side, missing details keep the
    fail-closed wait (a real fill may still be in flight)."""
    from entropy_bot.broker import PendingFillReconciliation

    cfg, meta, fees, _book = objects()
    market = _PhantomFilledMarket(position_szi="0.5")
    broker = make_live_broker(cfg, meta, fees, market)
    _clipped_reduce_only_ask(broker, oid=92)

    with pytest.raises(PendingFillReconciliation):
        asyncio.run(broker._reconcile_missing_tracked(set(), set()))
    assert "mm:ask:0" in broker.orders_by_role


def test_exit_cancel_releases_clipped_reduce_only_instead_of_stalling():
    """A clipped reduce-only remainder must not block halt/exit cancellation.

    Releasing that role lets cancel_roles finish and cancel sibling orders."""
    cfg, meta, fees, _book = objects()
    market = _PhantomFilledMarket(position_szi=None)
    broker = make_live_broker(cfg, meta, fees, market)
    _clipped_reduce_only_ask(broker, oid=93)

    class FakeExchange:
        bulk_cancel_by_cloid = object()

    broker.exchange = FakeExchange()

    async def fake_sdk(_fn, _reqs):
        return {"status": "ok"}

    broker._sdk = fake_sdk  # type: ignore[method-assign]
    asyncio.run(broker.cancel_roles(["mm:ask:0"]))

    assert broker.orders_by_role == {}
    assert 93 in broker._retired_orders_by_oid


# ---------------------------------------------------------------------------
# Interrupted place resolution
# ---------------------------------------------------------------------------


def test_interrupted_place_reserves_unknown_and_adopts_landed():
    """A transient unknownOid never releases a non-idempotent place.

    In a mixed batch, the positive cloid is adopted while the unresolved cloid
    keeps an oid-less role placeholder.  The batch raises and no later loop can
    submit a duplicate for either role.
    """
    cfg, meta, fees, book = objects()

    class ProbeMarket(FakeWsMarket):
        def __init__(self):
            self.calls: list[str] = []

        async def order_status(self, _address, cloid):
            self.calls.append(cloid)
            # First prepared cloid (the bid, probed first) never landed;
            # the other one is resting.
            if cloid == self.calls[0]:
                return {"status": "unknownOid"}
            return {
                "status": "order",
                "order": {
                    "status": "open",
                    "oid": 901,
                    "order": {"origSz": "1", "sz": "1"},
                },
            }

    market = ProbeMarket()
    broker = make_live_broker(cfg, meta, fees, market)

    class FakeExchange:
        bulk_orders = object()

    broker.exchange = FakeExchange()

    async def dying_sdk(_fn, _requests):
        raise RuntimeError("connection dropped mid-send")

    broker._sdk = dying_sdk  # type: ignore[method-assign]
    intents = [
        OrderIntent("mm:bid:0", True, 99.5, 1.0),
        OrderIntent("mm:ask:0", False, 100.5, 1.0),
    ]
    with pytest.raises(RuntimeError, match="下单结果不确定"):
        asyncio.run(broker.place(intents, book))

    # First cloid (…01) stays reserved; second is positively adopted.
    assert broker.orders_by_role["mm:bid:0"].oid is None
    assert broker.orders_by_role["mm:ask:0"].oid == 901
    # unknownOid was probed twice for confirmation.
    assert market.calls.count(market.calls[0]) == 2
    actions = broker.drain_order_events()
    assert len(actions) == 2
    assert [row["event"] for row in actions] == ["place", "place"]
    assert {row["outcome"] for row in actions} == {
        "send_uncertain",
        "reconciled_open",
    }
    assert broker.action_budget.session_spent == 2


def test_interrupted_place_unknown_then_late_open_adopts_same_cloid(monkeypatch):
    cfg, meta, fees, book = objects()

    class ProbeMarket(FakeWsMarket):
        async def order_status(self, _address, _cloid):
            return {"status": "unknownOid"}

    broker = make_live_broker(cfg, meta, fees, ProbeMarket())

    class FakeExchange:
        bulk_orders = object()

    broker.exchange = FakeExchange()
    sdk_calls = 0

    async def dying_sdk(_fn, _requests):
        nonlocal sdk_calls
        sdk_calls += 1
        raise RuntimeError("connection dropped mid-send")

    async def no_sleep(_seconds):
        return None

    broker._sdk = dying_sdk  # type: ignore[method-assign]
    monkeypatch.setattr("entropy_bot.broker.asyncio.sleep", no_sleep)
    with pytest.raises(RuntimeError, match="下单结果不确定"):
        asyncio.run(
            broker.place([OrderIntent("mm:bid:0", True, 99.5, 1.0)], book)
        )

    placeholder = broker.orders_by_role["mm:bid:0"]
    assert placeholder.oid is None
    broker._classify_official_orders(
        [{"coin": meta.coin, "cloid": placeholder.cloid, "oid": 902}]
    )

    assert broker.orders_by_role["mm:bid:0"] is placeholder
    assert placeholder.oid == 902
    assert broker._oid_to_role[902] == "mm:bid:0"
    assert placeholder.cloid not in broker._identity_pending_cloids
    events = broker.drain_order_events()
    assert [row["event"] for row in events] == ["place", "order_identity"]
    assert events[-1]["oid"] == 902
    assert broker.action_budget.session_spent == 1
    assert sdk_calls == 1


def test_interrupted_place_unknown_then_fill_is_attributed_once(monkeypatch):
    cfg, meta, fees, book = objects()

    class ProbeMarket(FakeWsMarket):
        async def order_status(self, _address, _cloid):
            return {"status": "unknownOid"}

    broker = make_live_broker(cfg, meta, fees, ProbeMarket())

    class FakeExchange:
        bulk_orders = object()

    broker.exchange = FakeExchange()
    sdk_calls = 0

    async def dying_sdk(_fn, _requests):
        nonlocal sdk_calls
        sdk_calls += 1
        raise RuntimeError("connection dropped mid-send")

    async def no_sleep(_seconds):
        return None

    broker._sdk = dying_sdk  # type: ignore[method-assign]
    monkeypatch.setattr("entropy_bot.broker.asyncio.sleep", no_sleep)
    with pytest.raises(RuntimeError, match="下单结果不确定"):
        asyncio.run(
            broker.place([OrderIntent("mm:bid:0", True, 99.5, 1.0)], book)
        )
    placeholder = broker.orders_by_role["mm:bid:0"]
    fill_row = {
        "coin": meta.coin,
        "cloid": placeholder.cloid,
        "oid": 903,
        "tid": "late-place-fill",
        "side": "B",
        "px": "99.5",
        "sz": "1",
        "fee": "0.01",
        "closedPnl": "0",
        "crossed": False,
        "time": 123456,
    }

    broker._ingest_fill_row(fill_row)
    broker._ingest_fill_row(fill_row)

    fills = broker.drain_fills()
    assert len(fills) == 1
    assert fills[0].order_role == "mm:bid:0"
    assert fills[0].cloid == placeholder.cloid
    assert 903 in broker._retired_orders_by_oid
    assert sdk_calls == 1


# ---------------------------------------------------------------------------
# Order lifecycle event ledger
# ---------------------------------------------------------------------------


def test_place_and_reject_emit_order_events():
    cfg, meta, fees, book = objects()
    broker = make_live_broker(cfg, meta, fees)

    class FakeExchange:
        bulk_orders = object()

    broker.exchange = FakeExchange()

    async def fake_sdk(_fn, _requests):
        return {
            "status": "ok",
            "response": {
                "data": {
                    "statuses": [
                        {"resting": {"oid": 501}},
                        {"error": "Post only order would have immediately matched"},
                    ]
                }
            },
        }

    broker._sdk = fake_sdk  # type: ignore[method-assign]
    intents = [
        OrderIntent("mm:bid:0", True, 99.5, 1.0),
        OrderIntent("mm:ask:0", False, 100.5, 1.0),
    ]
    asyncio.run(broker.place(intents, book))

    events = broker.drain_order_events()
    assert [e["event"] for e in events] == ["place", "place"]
    assert events[0]["role"] == "mm:bid:0" and events[0]["oid"] == 501
    assert events[0]["outcome"] == "resting"
    assert events[1]["outcome"] == "rejected"
    assert "immediately matched" in events[1]["reason"]
    assert broker.action_budget.session_spent == 2
    assert broker.drain_order_events() == []


def test_place_top_level_rejection_emits_exactly_one_spent_action():
    cfg, meta, fees, book = objects()
    broker = make_live_broker(cfg, meta, fees)

    class FakeExchange:
        bulk_orders = object()

    broker.exchange = FakeExchange()

    async def fake_sdk(_fn, _requests):
        return {"status": "err", "response": "insufficient margin"}

    broker._sdk = fake_sdk  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="被交易所拒绝"):
        asyncio.run(
            broker.place([OrderIntent("mm:bid:0", True, 99.5, 1.0)], book)
        )

    actions = broker.drain_order_events()
    assert len(actions) == 1
    assert actions[0]["event"] == "place"
    assert actions[0]["outcome"] == "rejected"
    assert broker.action_budget.session_spent == 1


def test_order_events_ledger_writes_jsonl(tmp_path):
    import json as _json

    from entropy_bot.storage import Ledger

    ledger = Ledger(tmp_path, "live", "market_maker", "io:SNDK", "sess1")
    ledger.record_order_events(
        [{"time_ms": 1, "event": "place", "role": "mm:bid:0"}]
    )
    rows = [
        _json.loads(line)
        for line in ledger.orders_path.read_text(encoding="utf-8").splitlines()
    ]
    assert rows == [
        {"session_id": "sess1", "time_ms": 1, "event": "place", "role": "mm:bid:0"}
    ]


# ---------------------------------------------------------------------------
# Config defaults
# ---------------------------------------------------------------------------


def test_new_runtime_keys_default_and_validate():
    cfg, _meta, _fees, _book = objects()
    rt = cfg.runtime
    assert rt.use_ws_user_feeds is True
    assert rt.rest_sweep_sec == 60.0
    assert rt.use_batch_modify is True
    assert rt.budget_poll_sec == 60.0
    assert rt.budget_soft_remaining > rt.budget_hard_remaining
    assert rt.auto_reserve_actions is False
    assert rt.reserve_actions_chunk == 10_000


# ---------------------------------------------------------------------------
# Engine reconcile routing
# ---------------------------------------------------------------------------


class RoutingBroker:
    """Records reconcile routing decisions without touching any exchange."""

    def __init__(self) -> None:
        from entropy_bot.models import AccountState

        self.orders_by_role: dict[str, TrackedOrder] = {}
        self.account = AccountState()
        self.requotes_modified = 0
        self.requotes_replaced = 0
        self.calls: list[tuple[str, list[str]]] = []

    def ws_healthy(self) -> bool:
        return True

    async def cancel_roles(self, roles):
        roles = list(roles)
        self.calls.append(("cancel", roles))
        for role in roles:
            self.orders_by_role.pop(role, None)

    async def modify_orders(self, pairs, _book):
        self.calls.append(("modify", [role for role, _ in pairs]))
        for role, intent in pairs:
            old = self.orders_by_role[role]
            self.orders_by_role[role] = TrackedOrder(
                role=role,
                cloid=old.cloid + "n",
                is_buy=intent.is_buy,
                price=intent.price,
                qty=intent.qty,
                reduce_only=intent.reduce_only,
                tif=intent.tif,
                created_ts=time.time(),
                oid=(old.oid or 0) + 100,
            )

    async def place(self, intents, _book):
        self.calls.append(("place", [intent.role for intent in intents]))
        for intent in intents:
            self.orders_by_role[intent.role] = TrackedOrder(
                role=intent.role,
                cloid="0x" + "9" * 32,
                is_buy=intent.is_buy,
                price=intent.price,
                qty=intent.qty,
                reduce_only=intent.reduce_only,
                tif=intent.tif,
                created_ts=time.time(),
                oid=999,
            )


def _routing_engine():
    from types import SimpleNamespace

    from entropy_bot.engine import Engine

    cfg, meta, _fees, book = objects()
    engine = Engine(cfg)
    engine.snapshot = SimpleNamespace(meta=meta, book=book)
    broker = RoutingBroker()
    engine.broker = broker  # type: ignore[assignment]
    engine.ledger = SimpleNamespace(save_state=lambda *_a, **_k: None)  # type: ignore[assignment]
    return engine, broker


def test_reconcile_routes_modify_cancel_and_place():
    from types import SimpleNamespace

    engine, broker = _routing_engine()
    broker.orders_by_role["mm:bid:0"] = tracked(
        "mm:bid:0", "0x" + "1" * 32, is_buy=True, oid=1
    )
    broker.orders_by_role["mm:ask:0"] = tracked(
        "mm:ask:0", "0x" + "2" * 32, is_buy=False, oid=2
    )
    broker.orders_by_role["mm:old"] = tracked(
        "mm:old", "0x" + "3" * 32, is_buy=True, oid=3
    )
    desired = [
        OrderIntent("mm:bid:0", True, 98.5, 1.0),   # defensive drift -> modify
        OrderIntent("mm:ask:0", False, 101.0, 1.0),  # unchanged -> untouched
        OrderIntent("mm:new", False, 102.0, 1.0),    # new -> place
    ]
    engine.strategy = SimpleNamespace(  # type: ignore[assignment]
        desired=lambda _snapshot, _account: desired,
        state_dict=lambda: {},
    )

    asyncio.run(engine._reconcile(force=True))

    assert ("cancel", ["mm:old"]) in broker.calls
    assert ("modify", ["mm:bid:0"]) in broker.calls
    assert ("place", ["mm:new"]) in broker.calls
    assert broker.orders_by_role["mm:bid:0"].price == 98.5
    assert broker.orders_by_role["mm:ask:0"].price == 101.0


def test_requote_hysteresis_ignores_tiny_size_drift_and_delays_chasing():
    engine, _broker = _routing_engine()
    old = tracked("mm:bid:0", "0x" + "a" * 32, is_buy=True, oid=7)

    # More than half a size step, but only 1% of the order: fixed-dollar
    # rounding must not bypass the price threshold.
    tiny_size_drift = OrderIntent("mm:bid:0", True, 99.005, 1.01)
    assert engine._needs_replace(old, tiny_size_drift) is False

    # Moving a bid away from incoming sells is defensive and can happen at the
    # normal threshold.
    defensive = OrderIntent("mm:bid:0", True, 98.98, 1.0)
    assert engine._needs_replace(old, defensive) is True

    # Chasing upward waits for both the wider threshold and a resting age.
    aggressive = OrderIntent("mm:bid:0", True, 99.05, 1.0)
    assert engine._needs_replace(old, aggressive) is False
    old.created_ts = time.time() - 4.0
    assert engine._needs_replace(old, aggressive) is True


def test_reconcile_routes_reduce_only_flip_to_cancel_and_place():
    """A reduce-only semantic change never goes through batchModify: the
    exchange rejects such modifies persistently, so it must take the plain
    cancel+place path even when the price barely moved."""
    from types import SimpleNamespace

    engine, broker = _routing_engine()
    broker.orders_by_role["mm:bid:0"] = tracked(
        "mm:bid:0", "0x" + "1" * 32, is_buy=True, oid=1
    )
    # Same price and qty as the tracked order: only the reduce_only flag flips.
    desired = [OrderIntent("mm:bid:0", True, 99.0, 1.0, reduce_only=True)]
    engine.strategy = SimpleNamespace(  # type: ignore[assignment]
        desired=lambda _snapshot, _account: desired,
        state_dict=lambda: {},
    )

    asyncio.run(engine._reconcile(force=True))

    kinds = [kind for kind, _ in broker.calls]
    assert "modify" not in kinds
    assert ("cancel", ["mm:bid:0"]) in broker.calls
    assert ("place", ["mm:bid:0"]) in broker.calls
    assert broker.orders_by_role["mm:bid:0"].reduce_only is True


def test_reconcile_hard_budget_only_cancels():
    from types import SimpleNamespace

    engine, broker = _routing_engine()
    budget = ActionBudget(soft_remaining=2500, hard_remaining=800)
    budget.apply_official({"cumVlm": "0", "nRequestsUsed": 9500, "nRequestsCap": 10000})
    assert budget.state == "hard"
    broker.action_budget = budget  # type: ignore[attr-defined]
    broker.orders_by_role["mm:bid:0"] = tracked(
        "mm:bid:0", "0x" + "1" * 32, is_buy=True, oid=1
    )
    broker.orders_by_role["mm:old"] = tracked(
        "mm:old", "0x" + "3" * 32, is_buy=True, oid=3
    )
    desired = [
        OrderIntent("mm:bid:0", True, 99.5, 1.0),
        OrderIntent("mm:new", False, 102.0, 1.0),
    ]
    engine.strategy = SimpleNamespace(  # type: ignore[assignment]
        desired=lambda _snapshot, _account: desired,
        state_dict=lambda: {},
    )

    asyncio.run(engine._reconcile(force=True))

    kinds = [kind for kind, _ in broker.calls]
    assert "cancel" in kinds
    assert "modify" not in kinds
    assert "place" not in kinds
