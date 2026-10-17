import asyncio
import threading
import time
from dataclasses import replace

import pytest

from entropy_bot.broker import (
    CANCEL_MAX_SUBMIT_ATTEMPTS,
    CANCEL_RETRY_INTERVAL_SEC,
    LiveBroker,
    PaperBroker,
    PendingCancelReconciliation,
    PendingFillReconciliation,
    PendingOrderReconciliation,
    UncertainIocOutcome,
    is_bot_cloid,
)
from entropy_bot.config import load_config
from entropy_bot.fees import build_fee_model
from entropy_bot.models import MarketMeta, OrderBook, OrderIntent, TrackedOrder


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


def test_paper_post_only_rejects_crossing():
    cfg, meta, fees, book = objects()
    broker = PaperBroker(cfg, meta, fees)
    with pytest.raises(RuntimeError):
        asyncio.run(broker.place([OrderIntent("bad", True, 100.1, 1)], book))


def test_paper_conservative_fill_and_fee_projection():
    cfg, meta, fees, book = objects()
    broker = PaperBroker(cfg, meta, fees)
    asyncio.run(broker.place([OrderIntent("bid", True, 99.8, 1)], book))
    order = broker.orders_by_role["bid"]
    assert is_bot_cloid(order.cloid)
    asyncio.run(broker.sync(book))
    assert not broker.drain_fills()
    book.apply_hl(
        [[{"px": "99.7", "sz": "10"}], [{"px": "99.8", "sz": "10"}]]
    )
    asyncio.run(broker.sync(book))
    fills = broker.drain_fills()
    assert len(fills) == 1
    assert fills[0].fee_usd > fills[0].projected_self_rebate > 0


def test_live_account_uses_entropy_dex_equity_and_withdrawable_only():
    cfg, meta, fees, book = objects()

    class FakeMarket:
        async def account_state(self, address):
            assert address == "0x" + "1" * 40
            return {
                "time": int(time.time() * 1000),
                "marginSummary": {"accountValue": "1000.25"},
                "withdrawable": "612.5",
                "assetPositions": [],
            }

    broker = LiveBroker(
        cfg,
        meta,
        fees,
        FakeMarket(),  # type: ignore[arg-type]
        "not-used-in-this-test",
        "0x" + "1" * 40,
    )
    asyncio.run(broker._sync_account(book))
    assert broker.account.equity_actual == 1000.25
    assert broker.account.withdrawable == 612.5


def test_official_order_drift_distinguishes_bot_orphans_from_foreign_orders():
    cfg, meta, fees, _book = objects()
    broker = LiveBroker(
        cfg,
        meta,
        fees,
        object(),  # type: ignore[arg-type]
        "not-used-in-this-test",
        "0x" + "1" * 40,
    )
    tracked_cloid = broker.cloids.next()
    orphan_cloid = broker.cloids.next()
    broker.orders_by_role["bid"] = TrackedOrder(
        role="bid",
        cloid=tracked_cloid,
        is_buy=True,
        price=99.0,
        qty=1.0,
        reduce_only=False,
        tif="Alo",
        created_ts=0.0,
        oid=11,
    )

    orphans = broker._classify_official_orders(
        [
            {"coin": meta.coin, "cloid": tracked_cloid, "oid": 11},
            {"coin": meta.coin, "cloid": orphan_cloid, "oid": 12},
            {"coin": meta.coin, "cloid": "0x1234", "oid": 13},
            {"coin": "io:OTHER", "cloid": "0x1234", "oid": 14},
        ]
    )

    assert orphans == [orphan_cloid]
    assert broker.official_untracked_bot_orders == 1
    assert broker.official_foreign_orders == 1


def test_cancel_bot_orphans_never_cancels_foreign_same_market_order():
    cfg, meta, fees, _book = objects()

    class FakeMarket:
        async def open_orders(self, _address):
            return [
                {"coin": meta.coin, "cloid": orphan_cloid, "oid": 12},
                {"coin": meta.coin, "cloid": "0x1234", "oid": 13},
            ]

    broker = LiveBroker(
        cfg,
        meta,
        fees,
        FakeMarket(),  # type: ignore[arg-type]
        "not-used-in-this-test",
        "0x" + "1" * 40,
    )
    orphan_cloid = broker.cloids.next()
    canceled: list[str] = []

    async def capture(cloids):
        canceled.extend(cloids)

    broker._cancel_cloids = capture  # type: ignore[method-assign]
    count = asyncio.run(broker.cancel_bot_orphans())

    assert count == 1
    assert canceled == [orphan_cloid]
    assert broker.official_untracked_bot_orders == 0
    assert broker.official_foreign_orders == 1


@pytest.mark.parametrize("clock_value", [0.0, 30.0])
def test_full_fill_against_stale_open_snapshot_pauses_then_recovers(monkeypatch, clock_value):
    monkeypatch.setattr(time, "monotonic", lambda: clock_value)
    cfg, meta, fees, book = objects()

    class FakeMarket:
        show_open = True
        open_calls = 0

        async def open_orders(self, _address):
            self.open_calls += 1
            if not self.show_open:
                return []
            return [{"coin": meta.coin, "cloid": cloid, "oid": 21}]

        async def fills_since(self, _address, _start_ms):
            return [
                {
                    "coin": meta.coin,
                    "oid": 21,
                    "tid": "same-cycle-fill",
                    "side": "B",
                    "px": "99.0",
                    "sz": "1.0",
                    "fee": "0.01",
                    "closedPnl": "0",
                    "crossed": False,
                    "time": 1000,
                }
            ]

    market = FakeMarket()
    broker = LiveBroker(
        cfg,
        meta,
        fees,
        market,  # type: ignore[arg-type]
        "not-used-in-this-test",
        "0x" + "1" * 40,
    )
    cloid = broker.cloids.next()
    broker.orders_by_role["bid"] = TrackedOrder(
        role="bid",
        cloid=cloid,
        is_buy=True,
        price=99.0,
        qty=1.0,
        reduce_only=False,
        tif="Alo",
        created_ts=0.0,
        oid=21,
    )
    broker._oid_to_role[21] = "bid"
    broker._last_account_sync = time.monotonic()

    with pytest.raises(PendingOrderReconciliation, match="开放委托快照"):
        asyncio.run(broker.sync(book))
    assert broker.orders_by_role == {}
    assert broker.official_untracked_bot_orders == 0
    assert cloid in broker._retired_open_since
    assert len(broker.drain_fills()) == 1

    # The pending guard must not poll again or release the engine to reconcile
    # before the normal refresh interval.
    with pytest.raises(PendingOrderReconciliation, match="暂停替换"):
        asyncio.run(broker.sync(book))
    assert market.open_calls == 1

    market.show_open = False
    broker._last_fill_sync = float("-inf")
    asyncio.run(broker.sync(book))
    assert cloid not in broker._retired_open_since
    assert broker.official_untracked_bot_orders == 0


@pytest.mark.parametrize("clock_value", [0.0, 30.0])
def test_retired_order_still_open_after_grace_becomes_cancelable_orphan(monkeypatch, clock_value):
    monkeypatch.setattr(time, "monotonic", lambda: clock_value)
    cfg, meta, fees, book = objects()

    class FakeMarket:
        async def open_orders(self, _address):
            return [{"coin": meta.coin, "cloid": cloid, "oid": 22}]

        async def fills_since(self, _address, _start_ms):
            return [
                {
                    "coin": meta.coin,
                    "oid": 22,
                    "tid": "persistent-open-fill",
                    "side": "B",
                    "px": "99.0",
                    "sz": "1.0",
                    "fee": "0.01",
                    "closedPnl": "0",
                    "crossed": False,
                    "time": 1000,
                }
            ]

    market = FakeMarket()
    broker = LiveBroker(
        cfg,
        meta,
        fees,
        market,  # type: ignore[arg-type]
        "not-used-in-this-test",
        "0x" + "1" * 40,
    )
    cloid = broker.cloids.next()
    broker.orders_by_role["bid"] = TrackedOrder(
        role="bid",
        cloid=cloid,
        is_buy=True,
        price=99.0,
        qty=1.0,
        reduce_only=False,
        tif="Alo",
        created_ts=0.0,
        oid=22,
    )
    broker._oid_to_role[22] = "bid"
    broker._last_account_sync = time.monotonic()

    with pytest.raises(PendingOrderReconciliation):
        asyncio.run(broker.sync(book))
    broker._retired_open_since[cloid] = time.monotonic() - 61.0
    broker._last_fill_sync = float("-inf")
    asyncio.run(broker.sync(book))
    assert broker.official_untracked_bot_orders == 1

    canceled: list[str] = []

    async def capture(cloids):
        canceled.extend(cloids)

    broker._cancel_cloids = capture  # type: ignore[method-assign]
    assert asyncio.run(broker.cancel_bot_orphans()) == 1
    assert canceled == [cloid]


@pytest.mark.parametrize(
    ("target", "target_role", "expected_vault"),
    [
        ("0x" + "1" * 40, {"role": "user"}, None),
        (
            "0x" + "2" * 40,
            {"role": "subAccount", "data": {"master": "0x" + "1" * 40}},
            "0x" + "2" * 40,
        ),
    ],
)
def test_live_signer_accepts_matching_api_wallet_and_sets_subaccount_vault(
    monkeypatch, target, target_role, expected_vault
):
    from eth_account import Account
    import hyperliquid.exchange

    cfg, meta, fees, _book = objects()
    # This test covers signer/account ownership, not leverage mutations.
    cfg = replace(cfg, market=replace(cfg.market, target_leverage=None))
    signer = Account.create()
    master = "0x" + "1" * 40

    class FakeMarket:
        async def user_role(self, address):
            if address.lower() == signer.address.lower():
                return {"role": "agent", "data": {"user": master}}
            assert address.lower() == target.lower()
            return target_role

        async def user_abstraction(self, _address):
            return "disabled"

        async def fills_since(self, _address, _start_ms):
            return []

        async def open_orders(self, _address):
            return []

    class FakeExchange:
        def __init__(self, wallet, **kwargs):
            assert threading.current_thread() is not threading.main_thread()
            self.wallet = wallet
            self.kwargs = kwargs

    monkeypatch.setattr(hyperliquid.exchange, "Exchange", FakeExchange)
    broker = LiveBroker(
        cfg,
        meta,
        fees,
        FakeMarket(),  # type: ignore[arg-type]
        signer.key.hex(),
        target,
    )
    asyncio.run(broker.start())
    assert broker.exchange.kwargs["vault_address"] == expected_vault
    assert broker._rate_limit_address == target.lower()
    from entropy_bot.signing_lock import SignerProcessLock
    assert broker._signing_lock.path == SignerProcessLock(signer.address).path


@pytest.mark.parametrize("use_subaccount", [False, True])
def test_live_signer_accepts_main_wallet_for_own_account_or_subaccount(
    monkeypatch, use_subaccount
):
    from eth_account import Account
    import hyperliquid.exchange

    cfg, meta, fees, _book = objects()
    cfg = replace(cfg, market=replace(cfg.market, target_leverage=None))
    signer = Account.create()
    target = "0x" + "2" * 40 if use_subaccount else signer.address

    class FakeMarket:
        async def user_role(self, address):
            if address.lower() == signer.address.lower():
                return {"role": "user"}
            assert use_subaccount and address.lower() == target.lower()
            return {"role": "subAccount", "data": {"master": signer.address}}

        async def user_abstraction(self, _address):
            return "disabled"

        async def fills_since(self, _address, _start_ms):
            return []

        async def open_orders(self, _address):
            return []

    class FakeExchange:
        def __init__(self, wallet, **kwargs):
            self.wallet = wallet
            self.kwargs = kwargs

    monkeypatch.setattr(hyperliquid.exchange, "Exchange", FakeExchange)
    broker = LiveBroker(
        cfg,
        meta,
        fees,
        FakeMarket(),  # type: ignore[arg-type]
        signer.key.hex(),
        target,
    )
    asyncio.run(broker.start())
    assert broker.exchange.kwargs["account_address"].lower() == target.lower()
    assert broker.exchange.kwargs["vault_address"] == (
        target.lower() if use_subaccount else None
    )


def test_live_signer_rejects_main_wallet_for_unrelated_account(monkeypatch):
    from eth_account import Account

    cfg, meta, fees, _book = objects()
    signer = Account.create()
    unrelated = "0x" + "9" * 40

    class FakeMarket:
        async def user_role(self, address):
            if address.lower() == signer.address.lower():
                return {"role": "user"}
            assert address.lower() == unrelated.lower()
            return {"role": "user"}

        async def user_abstraction(self, _address):
            return "disabled"

    broker = LiveBroker(
        cfg,
        meta,
        fees,
        FakeMarket(),  # type: ignore[arg-type]
        signer.key.hex(),
        unrelated,
    )
    with pytest.raises(RuntimeError, match="不匹配"):
        asyncio.run(broker.start())


def test_live_startup_quiet_gate_blocks_external_orders_and_queries_each_once():
    cfg, meta, fees, _book = objects()

    class FakeMarket:
        def __init__(self):
            self.calls = []

        async def fills_since(self, _address, _start_ms):
            return [
                {"coin": "io:SNDK", "oid": 11},
                {"coin": "io:SNDK", "oid": 11},
                {"coin": "io:SNDK", "oid": 22},
                {"coin": "io:OTHER", "oid": 33},
            ]

        async def order_status(self, _address, oid):
            self.calls.append(oid)
            return {
                "status": "order",
                "order": {"status": "filled", "order": {"cloid": "0x1234"}},
            }

    market = FakeMarket()
    broker = LiveBroker(
        cfg,
        meta,
        fees,
        market,  # type: ignore[arg-type]
        "not-used-in-this-test",
        "0x" + "1" * 40,
    )

    asyncio.run(broker._assert_recent_external_activity_quiet())
    assert market.calls == [11, 22]


def test_live_startup_quiet_gate_allows_bot_owned_recent_fills():
    cfg, meta, fees, _book = objects()
    bot_cloid = LiveBroker(
        cfg,
        meta,
        fees,
        object(),  # type: ignore[arg-type]
        "not-used-in-this-test",
        "0x" + "1" * 40,
    ).cloids.next()

    class FakeMarket:
        async def fills_since(self, _address, _start_ms):
            return [{"coin": "io:SNDK", "oid": 11}]

        async def order_status(self, _address, _oid):
            return {
                "status": "order",
                "order": {"status": "filled", "order": {"cloid": bot_cloid}},
            }

    broker = LiveBroker(
        cfg,
        meta,
        fees,
        FakeMarket(),  # type: ignore[arg-type]
        "not-used-in-this-test",
        "0x" + "1" * 40,
    )

    asyncio.run(broker._assert_recent_external_activity_quiet())


def test_live_start_blocks_foreign_open_order_before_exchange_or_leverage_mutation(
    monkeypatch,
):
    from eth_account import Account
    import hyperliquid.exchange as exchange_module

    cfg, meta, fees, _book = objects()
    signer = Account.create()
    calls = {"exchange_constructed": 0, "open_orders": 0}

    class FakeMarket:
        async def user_role(self, address):
            assert address.lower() == signer.address.lower()
            return {"role": "user"}

        async def user_abstraction(self, _address):
            return "disabled"

        async def fills_since(self, _address, _start_ms):
            return []

        async def open_orders(self, _address):
            calls["open_orders"] += 1
            return [{"coin": meta.coin, "cloid": "0x1234", "oid": 91}]

        async def active_asset_data(self, _address):
            raise AssertionError("foreign-order gate must run before leverage read")

    class FakeExchange:
        def __init__(self, *_args, **_kwargs):
            calls["exchange_constructed"] += 1
            raise AssertionError("signing client must not exist before foreign-order gate")

    monkeypatch.setattr(exchange_module, "Exchange", FakeExchange)
    broker = LiveBroker(
        cfg,
        meta,
        fees,
        FakeMarket(),  # type: ignore[arg-type]
        signer.key.hex(),
        signer.address,
    )

    with pytest.raises(RuntimeError, match="1 笔外部同市场订单"):
        asyncio.run(broker.start())
    assert calls == {"exchange_constructed": 0, "open_orders": 1}
    assert broker.exchange is None


def test_live_unified_account_uses_spot_usdc_and_active_asset_capacity():
    cfg, meta, fees, book = objects()

    class FakeMarket:
        async def account_state(self, _address):
            return {
                "time": int(time.time() * 1000),
                "marginSummary": {"accountValue": "0.0"},
                "withdrawable": "0.0",
                "assetPositions": [
                    {
                        "position": {
                            "coin": "io:SNDK",
                            "szi": "0.01",
                            "entryPx": "100.0",
                            "unrealizedPnl": "1.25",
                            "liquidationPx": "50.0",
                        }
                    }
                ],
            }

        async def spot_state(self, _address):
            return {
                "balances": [
                    {"coin": "USDC", "total": "499.80", "hold": "10.0"}
                ]
            }

        async def active_asset_data(self, _address):
            return {
                "availableToTrade": ["480.0", "470.0"],
                "leverage": {"type": "isolated", "value": 3},
            }

    broker = LiveBroker(
        cfg,
        meta,
        fees,
        FakeMarket(),  # type: ignore[arg-type]
        "not-used-in-this-test",
        "0x" + "1" * 40,
    )
    broker.account_abstraction = "unifiedAccount"
    asyncio.run(broker._sync_account(book))

    assert broker.account.equity_actual == pytest.approx(501.05)
    assert broker.account.withdrawable == 470.0
    assert broker.account.available_to_buy_usd == 480.0
    assert broker.account.available_to_sell_usd == 470.0
    assert broker.account.collateral_source == "unified_spot_usdc"
    assert broker.official_leverage_type == "isolated"
    assert broker.official_leverage_value == 3


@pytest.mark.parametrize("clock_value", [0.0, 30.0])
def test_live_sync_does_not_repeat_all_info_calls_each_engine_tick(monkeypatch, clock_value):
    monkeypatch.setattr(time, "monotonic", lambda: clock_value)
    cfg, meta, fees, book = objects()

    class FakeMarket:
        def __init__(self):
            self.calls = {"fills": 0, "orders": 0, "account": 0}

        async def fills_since(self, _address, _start_ms):
            self.calls["fills"] += 1
            return []

        async def open_orders(self, _address):
            self.calls["orders"] += 1
            return []

        async def account_state(self, _address):
            self.calls["account"] += 1
            return {
                "time": int(time.time() * 1000),
                "marginSummary": {"accountValue": "100.0"},
                "withdrawable": "100.0",
                "assetPositions": [],
            }

    market = FakeMarket()
    broker = LiveBroker(
        cfg,
        meta,
        fees,
        market,  # type: ignore[arg-type]
        "not-used-in-this-test",
        "0x" + "1" * 40,
    )

    async def twice():
        await broker.sync(book)
        await broker.sync(book)

    asyncio.run(twice())

    assert market.calls == {"fills": 1, "orders": 1, "account": 1}


@pytest.mark.parametrize("clock_value", [0.0, 30.0])
def test_quiet_live_sync_polls_orders_without_reloading_high_weight_fills(monkeypatch, clock_value):
    monkeypatch.setattr(time, "monotonic", lambda: clock_value)
    cfg, meta, fees, book = objects()

    class FakeMarket:
        def __init__(self):
            self.calls = {"fills": 0, "orders": 0, "account": 0}

        async def fills_since(self, _address, _start_ms):
            self.calls["fills"] += 1
            return []

        async def open_orders(self, _address):
            self.calls["orders"] += 1
            return []

        async def account_state(self, _address):
            self.calls["account"] += 1
            return {
                "time": int(time.time() * 1000),
                "marginSummary": {"accountValue": "100"},
                "withdrawable": "100",
                "assetPositions": [],
            }

    market = FakeMarket()
    broker = LiveBroker(
        cfg,
        meta,
        fees,
        market,  # type: ignore[arg-type]
        "not-used-in-this-test",
        "0x" + "1" * 40,
    )

    async def run():
        await broker.sync(book)
        broker._last_fill_sync -= cfg.runtime.fill_refresh_sec + 1
        await broker.sync(book)

    asyncio.run(run())

    assert market.calls == {"fills": 1, "orders": 2, "account": 1}


def test_clean_cancel_uses_low_weight_status_without_fetching_fills():
    cfg, meta, fees, _book = objects()

    class FakeMarket:
        async def order_status(self, _address, _cloid):
            return {
                "status": "order",
                "order": {
                    "status": "canceled",
                    "order": {"origSz": "1", "sz": "1"},
                },
            }

        async def fills_since(self, _address, _start_ms):
            raise AssertionError("zero-fill cancellation must not fetch userFills")

    broker = LiveBroker(
        cfg,
        meta,
        fees,
        FakeMarket(),  # type: ignore[arg-type]
        "not-used-in-this-test",
        "0x" + "1" * 40,
    )
    broker.orders_by_role["bid"] = TrackedOrder(
        role="bid",
        cloid="0x" + "1" * 32,
        is_buy=True,
        price=99.0,
        qty=1.0,
        reduce_only=False,
        tif="Alo",
        created_ts=0.0,
    )

    async def no_op_cancel(_cloids):
        return None

    broker._cancel_cloids = no_op_cancel  # type: ignore[method-assign]
    asyncio.run(broker.cancel_roles(["bid"]))
    assert broker.orders_by_role == {}


def test_pending_cancel_waits_without_resending_and_then_clears():
    cfg, meta, fees, _book = objects()
    cloid = "0x" + "6" * 32

    class FakeMarket:
        is_open = True

        async def open_orders(self, _address):
            if not self.is_open:
                return []
            return [{"coin": "io:SNDK", "cloid": cloid}]

        async def order_status(self, _address, _cloid):
            return {
                "status": "order",
                "order": {
                    "status": "canceled",
                    "order": {"origSz": "1", "sz": "1"},
                },
            }

        async def fills_since(self, _address, _start_ms):
            raise AssertionError("zero-fill cancellation must not fetch userFills")

    class FakeExchange:
        bulk_cancel_by_cloid = object()

    market = FakeMarket()
    broker = LiveBroker(
        cfg,
        meta,
        fees,
        market,  # type: ignore[arg-type]
        "not-used-in-this-test",
        "0x" + "1" * 40,
    )
    broker.exchange = FakeExchange()
    broker.orders_by_role["bid"] = TrackedOrder(
        role="bid",
        cloid=cloid,
        is_buy=True,
        price=99.0,
        qty=1.0,
        reduce_only=False,
        tif="Alo",
        created_ts=0.0,
    )
    sends = 0

    async def fake_sdk(_fn, _reqs):
        nonlocal sends
        sends += 1
        return {"status": "ok"}

    broker._sdk = fake_sdk  # type: ignore[method-assign]
    with pytest.raises(PendingCancelReconciliation):
        asyncio.run(broker.cancel_roles(["bid"]))
    with pytest.raises(PendingCancelReconciliation):
        asyncio.run(broker.cancel_roles(["bid"]))
    assert sends == 1
    assert "bid" in broker.orders_by_role

    market.is_open = False
    asyncio.run(broker.cancel_roles(["bid"]))
    assert sends == 1
    assert broker.orders_by_role == {}
    assert cloid not in broker._cancel_pending_since
    cancel_events = [
        row for row in broker.drain_order_events() if row["event"] == "cancel"
    ]
    assert len(cancel_events) == 1
    assert broker.action_budget.session_spent == 1


def test_timed_out_cancel_retries_only_after_official_open_then_reconciles():
    cfg, meta, fees, _book = objects()
    cloid = "0x" + "b" * 32

    class FakeMarket:
        is_open = True

        async def open_orders(self, _address):
            if self.is_open:
                return [{"coin": "io:SNDK", "cloid": cloid}]
            return []

        async def order_status(self, _address, _cloid):
            return {
                "status": "order",
                "order": {
                    "status": "open" if self.is_open else "canceled",
                    "order": {"origSz": "1", "sz": "1"},
                },
            }

        async def fills_since(self, _address, _start_ms):
            raise AssertionError("zero-fill cancellation must not fetch userFills")

    class FakeExchange:
        bulk_cancel_by_cloid = object()

    market = FakeMarket()
    broker = LiveBroker(
        cfg,
        meta,
        fees,
        market,  # type: ignore[arg-type]
        "not-used-in-this-test",
        "0x" + "1" * 40,
    )
    broker.exchange = FakeExchange()
    broker.orders_by_role["bid"] = TrackedOrder(
        role="bid",
        cloid=cloid,
        is_buy=True,
        price=99.0,
        qty=1.0,
        reduce_only=False,
        tif="Alo",
        created_ts=0.0,
    )
    sends = 0

    async def flaky_sdk(_fn, _reqs):
        nonlocal sends
        sends += 1
        if sends == 1:
            raise TimeoutError("first cancel outcome unknown")
        market.is_open = False
        return {"status": "ok"}

    broker._sdk = flaky_sdk  # type: ignore[method-assign]
    with pytest.raises(PendingCancelReconciliation):
        asyncio.run(broker.cancel_roles(["bid"]))
    assert sends == 1
    assert broker._cancel_submit_attempts[cloid] == 1

    # Merely polling again is not enough to resend; the bounded cadence must
    # have elapsed as well as a fresh official-open proof.
    broker._cancel_last_submit_at[cloid] -= CANCEL_RETRY_INTERVAL_SEC + 0.1
    asyncio.run(broker.cancel_roles(["bid"]))

    assert sends == 2
    assert broker.orders_by_role == {}
    assert cloid not in broker._cancel_pending_since
    assert cloid not in broker._cancel_last_submit_at
    assert cloid not in broker._cancel_submit_attempts
    cancel_events = [
        row for row in broker.drain_order_events() if row["event"] == "cancel"
    ]
    assert len(cancel_events) == 2
    assert cancel_events[1]["reason"] == "official_still_open_retry"
    assert cancel_events[1]["attempt"] == 2
    assert broker.action_budget.session_spent == 2


def test_pending_cancel_halts_after_reconciliation_grace():
    cfg, meta, fees, _book = objects()
    cloid = "0x" + "7" * 32

    class FakeMarket:
        async def open_orders(self, _address):
            return [{"coin": "io:SNDK", "cloid": cloid}]

    broker = LiveBroker(
        cfg,
        meta,
        fees,
        FakeMarket(),  # type: ignore[arg-type]
        "not-used-in-this-test",
        "0x" + "1" * 40,
    )
    broker.exchange = object()
    broker.orders_by_role["ask"] = TrackedOrder(
        role="ask",
        cloid=cloid,
        is_buy=False,
        price=101.0,
        qty=1.0,
        reduce_only=False,
        tif="Alo",
        created_ts=0.0,
    )
    broker._cancel_pending_since[cloid] = time.monotonic() - 61.0

    async def no_resend(_fn, _reqs):
        raise AssertionError("pending cancel must not be resent")

    broker._sdk = no_resend  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="超过 60 秒"):
        asyncio.run(broker.cancel_roles(["ask"]))


def test_pending_cancel_attempt_cap_never_blindly_resends():
    cfg, meta, fees, _book = objects()
    cloid = "0x" + "c" * 32

    class FakeMarket:
        async def open_orders(self, _address):
            return [{"coin": "io:SNDK", "cloid": cloid}]

    broker = LiveBroker(
        cfg,
        meta,
        fees,
        FakeMarket(),  # type: ignore[arg-type]
        "not-used-in-this-test",
        "0x" + "1" * 40,
    )
    broker.exchange = object()
    broker._cancel_pending_since[cloid] = time.monotonic() - 20.0
    broker._cancel_last_submit_at[cloid] = time.monotonic() - 10.0
    broker._cancel_submit_attempts[cloid] = CANCEL_MAX_SUBMIT_ATTEMPTS

    async def no_resend(_fn, _reqs):
        raise AssertionError("attempt cap must prevent another signed cancel")

    broker._sdk = no_resend  # type: ignore[method-assign]
    with pytest.raises(PendingCancelReconciliation, match="3/3"):
        asyncio.run(broker._cancel_cloids([cloid]))
    assert broker.action_budget.session_spent == 0


@pytest.mark.parametrize("status_state", ["canceled", "unknown"])
def test_cancel_retry_requires_open_orders_and_order_status_open(status_state):
    cfg, meta, fees, _book = objects()
    cloid = "0x" + "d" * 32

    class FakeMarket:
        async def open_orders(self, _address):
            # This view is deliberately stale for the terminal case.
            return [{"coin": "io:SNDK", "cloid": cloid}]

        async def order_status(self, _address, _cloid):
            if status_state == "unknown":
                return {"status": "unknownOid"}
            return {
                "status": "order",
                "order": {
                    "status": status_state,
                    "order": {"origSz": "1", "sz": "1"},
                },
            }

    broker = LiveBroker(
        cfg,
        meta,
        fees,
        FakeMarket(),  # type: ignore[arg-type]
        "not-used-in-this-test",
        "0x" + "1" * 40,
    )
    broker.exchange = object()
    now = time.monotonic()
    broker._cancel_pending_since[cloid] = now - 10.0
    broker._cancel_last_submit_at[cloid] = now - CANCEL_RETRY_INTERVAL_SEC - 0.1
    broker._cancel_submit_attempts[cloid] = 1

    async def no_resend(_fn, _reqs):
        raise AssertionError("non-open orderStatus must prevent a retry")

    broker._sdk = no_resend  # type: ignore[method-assign]
    if status_state == "unknown":
        with pytest.raises(PendingCancelReconciliation):
            asyncio.run(broker._cancel_cloids([cloid]))
    else:
        asyncio.run(broker._cancel_cloids([cloid]))
    assert broker._cancel_submit_attempts[cloid] == 1
    assert broker.action_budget.session_spent == 0


def test_cancel_terminal_cleanup_handles_prior_missing_order_state():
    cfg, meta, fees, _book = objects()
    cloid = "0x" + "6" * 32

    class FakeMarket:
        async def open_orders(self, _address):
            return []

        async def order_status(self, _address, _cloid):
            return {
                "status": "order",
                "order": {
                    "status": "canceled",
                    "order": {"origSz": "1", "sz": "1"},
                },
            }

    broker = LiveBroker(
        cfg,
        meta,
        fees,
        FakeMarket(),  # type: ignore[arg-type]
        "not-used-in-this-test",
        "0x" + "1" * 40,
    )
    broker.exchange = object()
    broker.orders_by_role["bid"] = TrackedOrder(
        role="bid",
        cloid=cloid,
        is_buy=True,
        price=99.0,
        qty=1.0,
        reduce_only=False,
        tif="Alo",
        created_ts=0.0,
    )
    broker._missing_since[cloid] = time.monotonic() - 5.0

    asyncio.run(broker.cancel_roles(["bid"]))

    assert broker.orders_by_role == {}
    assert cloid not in broker._missing_since


def test_cancel_waits_for_transient_open_order_status():
    cfg, meta, fees, _book = objects()

    class FakeMarket:
        def __init__(self):
            self.states = ["open", "open", "canceled"]
            self.calls = 0

        async def order_status(self, _address, _cloid):
            state = self.states[min(self.calls, len(self.states) - 1)]
            self.calls += 1
            return {
                "status": "order",
                "order": {
                    "status": state,
                    "order": {"origSz": "1", "sz": "1"},
                },
            }

        async def fills_since(self, _address, _start_ms):
            raise AssertionError("zero-fill cancellation must not fetch userFills")

    market = FakeMarket()
    broker = LiveBroker(
        cfg,
        meta,
        fees,
        market,  # type: ignore[arg-type]
        "not-used-in-this-test",
        "0x" + "1" * 40,
    )
    broker.orders_by_role["ask"] = TrackedOrder(
        role="ask",
        cloid="0x" + "2" * 32,
        is_buy=False,
        price=101.0,
        qty=1.0,
        reduce_only=False,
        tif="Alo",
        created_ts=0.0,
    )

    async def no_op_cancel(_cloids):
        return None

    broker._cancel_cloids = no_op_cancel  # type: ignore[method-assign]
    asyncio.run(broker.cancel_roles(["ask"]))
    assert market.calls == 3
    assert broker.orders_by_role == {}


def test_missing_open_order_gets_grace_until_status_turns_terminal():
    cfg, meta, fees, _book = objects()

    class FakeMarket:
        state = "open"

        async def open_orders(self, _address):
            return []

        async def order_status(self, _address, _cloid):
            return {
                "status": "order",
                "order": {
                    "status": self.state,
                    "order": {"origSz": "1", "sz": "1"},
                },
            }

    market = FakeMarket()
    broker = LiveBroker(
        cfg,
        meta,
        fees,
        market,  # type: ignore[arg-type]
        "not-used-in-this-test",
        "0x" + "1" * 40,
    )
    cloid = "0x" + "3" * 32
    broker.orders_by_role["bid"] = TrackedOrder(
        role="bid",
        cloid=cloid,
        is_buy=True,
        price=99.0,
        qty=1.0,
        reduce_only=False,
        tif="Alo",
        created_ts=0.0,
    )

    asyncio.run(broker._sync_open_orders())
    assert "bid" in broker.orders_by_role
    assert cloid in broker._missing_since

    market.state = "canceled"
    asyncio.run(broker._sync_open_orders())
    assert broker.orders_by_role == {}
    assert cloid not in broker._missing_since


def test_missing_open_order_after_fast_grace_is_pending_until_sixty_seconds():
    cfg, meta, fees, _book = objects()

    class FakeMarket:
        async def open_orders(self, _address):
            return []

        async def order_status(self, _address, _cloid):
            return {
                "status": "order",
                "order": {
                    "status": "open",
                    "order": {"origSz": "1", "sz": "1"},
                },
            }

    broker = LiveBroker(
        cfg,
        meta,
        fees,
        FakeMarket(),  # type: ignore[arg-type]
        "not-used-in-this-test",
        "0x" + "1" * 40,
    )
    cloid = "0x" + "8" * 32
    broker.orders_by_role["bid"] = TrackedOrder(
        role="bid",
        cloid=cloid,
        is_buy=True,
        price=99.0,
        qty=1.0,
        reduce_only=False,
        tif="Alo",
        created_ts=0.0,
    )
    broker._missing_since[cloid] = time.monotonic() - 16.0

    with pytest.raises(PendingOrderReconciliation, match="暂停替换"):
        asyncio.run(broker._sync_open_orders())
    assert "bid" in broker.orders_by_role


def test_missing_open_order_halts_only_after_sixty_second_disagreement():
    cfg, meta, fees, _book = objects()

    class FakeMarket:
        async def open_orders(self, _address):
            return []

        async def order_status(self, _address, _cloid):
            return {
                "status": "order",
                "order": {
                    "status": "open",
                    "order": {"origSz": "1", "sz": "1"},
                },
            }

    broker = LiveBroker(
        cfg,
        meta,
        fees,
        FakeMarket(),  # type: ignore[arg-type]
        "not-used-in-this-test",
        "0x" + "1" * 40,
    )
    cloid = "0x" + "9" * 32
    broker.orders_by_role["ask"] = TrackedOrder(
        role="ask",
        cloid=cloid,
        is_buy=False,
        price=101.0,
        qty=1.0,
        reduce_only=False,
        tif="Alo",
        created_ts=0.0,
    )
    broker._missing_since[cloid] = time.monotonic() - 61.0

    with pytest.raises(RuntimeError, match="持续矛盾超过 60 秒"):
        asyncio.run(broker._sync_open_orders())
    assert "ask" in broker.orders_by_role


def test_cancel_absent_but_status_open_keeps_one_shot_guard():
    cfg, meta, fees, _book = objects()
    cloid = "0x" + "a" * 32

    class FakeMarket:
        async def open_orders(self, _address):
            return []

        async def order_status(self, _address, _cloid):
            return {
                "status": "order",
                "order": {
                    "status": "open",
                    "order": {"origSz": "1", "sz": "1"},
                },
            }

    broker = LiveBroker(
        cfg,
        meta,
        fees,
        FakeMarket(),  # type: ignore[arg-type]
        "not-used-in-this-test",
        "0x" + "1" * 40,
    )
    broker.exchange = object()
    broker.orders_by_role["ask"] = TrackedOrder(
        role="ask",
        cloid=cloid,
        is_buy=False,
        price=101.0,
        qty=1.0,
        reduce_only=False,
        tif="Alo",
        created_ts=0.0,
    )
    broker._cancel_pending_since[cloid] = time.monotonic() - 16.0
    broker._missing_since[cloid] = time.monotonic() - 16.0

    async def no_resend(_fn, _reqs):
        raise AssertionError("cancel must not be resent while views disagree")

    broker._sdk = no_resend  # type: ignore[method-assign]
    with pytest.raises(PendingCancelReconciliation, match="不会重发撤单"):
        asyncio.run(broker.cancel_roles(["ask"]))
    assert cloid in broker._cancel_pending_since
    assert "ask" in broker.orders_by_role


def test_canceled_size_delta_refreshes_position_and_retires_mapping():
    cfg, meta, fees, _book = objects()

    class FakeMarket:
        async def open_orders(self, _address):
            return []

        async def order_status(self, _address, _cloid):
            return {
                "status": "order",
                "order": {
                    "status": "canceled",
                    "order": {"origSz": "1", "sz": "0.5"},
                },
            }

        async def account_state(self, _address):
            return {
                "time": int(time.time() * 1000),
                "assetPositions": [
                    {
                        "position": {
                            "coin": "io:SNDK",
                            "szi": "0.25",
                            "entryPx": "99",
                            "unrealizedPnl": "1.5",
                            "liquidationPx": None,
                        }
                    }
                ]
            }

    broker = LiveBroker(
        cfg,
        meta,
        fees,
        FakeMarket(),  # type: ignore[arg-type]
        "not-used-in-this-test",
        "0x" + "1" * 40,
    )
    cloid = "0x" + "4" * 32
    broker.orders_by_role["bid"] = TrackedOrder(
        role="bid",
        cloid=cloid,
        is_buy=True,
        price=99.0,
        qty=1.0,
        reduce_only=False,
        tif="Alo",
        created_ts=0.0,
        oid=71,
    )
    broker._oid_to_role[71] = "bid"

    asyncio.run(broker._sync_open_orders())
    assert "bid" not in broker.orders_by_role
    assert 71 in broker._retired_orders_by_oid
    assert broker.account.position_qty == pytest.approx(0.25)


def test_canceled_size_delta_does_not_halt_after_reconciliation_grace():
    cfg, meta, fees, _book = objects()

    class FakeMarket:
        async def open_orders(self, _address):
            return []

        async def order_status(self, _address, _cloid):
            return {
                "status": "order",
                "order": {
                    "status": "canceled",
                    "order": {"origSz": "1", "sz": "0.5"},
                },
            }

        async def account_state(self, _address):
            return {"assetPositions": [], "time": int(time.time() * 1000)}

    broker = LiveBroker(
        cfg,
        meta,
        fees,
        FakeMarket(),  # type: ignore[arg-type]
        "not-used-in-this-test",
        "0x" + "1" * 40,
    )
    cloid = "0x" + "5" * 32
    broker.orders_by_role["bid"] = TrackedOrder(
        role="bid",
        cloid=cloid,
        is_buy=True,
        price=99.0,
        qty=1.0,
        reduce_only=False,
        tif="Alo",
        created_ts=0.0,
        oid=72,
    )
    broker._oid_to_role[72] = "bid"
    broker._missing_since[cloid] = time.monotonic() - 301.0

    asyncio.run(broker._sync_open_orders())
    assert "bid" not in broker.orders_by_role
    assert 72 in broker._retired_orders_by_oid


def test_canceled_size_delta_no_longer_waits_after_previous_grace():
    cfg, meta, fees, _book = objects()

    class FakeMarket:
        async def open_orders(self, _address):
            return []

        async def order_status(self, _address, _cloid):
            return {
                "status": "order",
                "order": {
                    "status": "canceled",
                    "order": {"origSz": "1", "sz": "0.5"},
                },
            }

        async def account_state(self, _address):
            return {"assetPositions": [], "time": int(time.time() * 1000)}

    broker = LiveBroker(
        cfg,
        meta,
        fees,
        FakeMarket(),  # type: ignore[arg-type]
        "not-used-in-this-test",
        "0x" + "1" * 40,
    )
    cloid = "0x" + "b" * 32
    broker.orders_by_role["bid"] = TrackedOrder(
        role="bid",
        cloid=cloid,
        is_buy=True,
        price=99.0,
        qty=1.0,
        reduce_only=False,
        tif="Alo",
        created_ts=0.0,
        oid=73,
    )
    broker._oid_to_role[73] = "bid"
    broker._missing_since[cloid] = time.monotonic() - 181.0

    asyncio.run(broker._sync_open_orders())
    assert "bid" not in broker.orders_by_role
    assert 73 in broker._retired_orders_by_oid


def test_live_fill_updates_local_position_before_account_refresh():
    cfg, meta, fees, _book = objects()

    class FakeMarket:
        async def fills_since(self, _address, _start_ms):
            return [
                {
                    "coin": "io:SNDK",
                    "oid": 77,
                    "tid": "fill-1",
                    "side": "A",
                    "px": "100.0",
                    "sz": "0.1",
                    "fee": "0.001",
                    "closedPnl": "0",
                    "crossed": False,
                    "time": 1000,
                }
            ]

    broker = LiveBroker(
        cfg,
        meta,
        fees,
        FakeMarket(),  # type: ignore[arg-type]
        "not-used-in-this-test",
        "0x" + "1" * 40,
    )
    broker.account.position_qty = -0.2
    broker.orders_by_role["ask"] = TrackedOrder(
        role="ask",
        cloid="0x" + "4" * 32,
        is_buy=False,
        price=100.0,
        qty=0.1,
        reduce_only=False,
        tif="Alo",
        created_ts=0.0,
        oid=77,
    )
    broker._oid_to_role[77] = "ask"

    asyncio.run(broker._sync_fills())
    assert broker.account.position_qty == pytest.approx(-0.3)

    # The overlapping fill window must not double-apply an already seen fill.
    asyncio.run(broker._sync_fills())
    assert broker.account.position_qty == pytest.approx(-0.3)


def test_late_fill_after_clean_cancel_uses_retired_order_mapping():
    cfg, meta, fees, _book = objects()

    class FakeMarket:
        delayed_fills = []

        async def open_orders(self, _address):
            return []

        async def order_status(self, _address, _cloid):
            return {
                "status": "order",
                "order": {
                    "status": "canceled",
                    "order": {"origSz": "0.1", "sz": "0.1"},
                },
            }

        async def fills_since(self, _address, _start_ms):
            return self.delayed_fills

    market = FakeMarket()
    broker = LiveBroker(
        cfg,
        meta,
        fees,
        market,  # type: ignore[arg-type]
        "not-used-in-this-test",
        "0x" + "1" * 40,
    )
    order = TrackedOrder(
        role="ask",
        cloid="0x" + "7" * 32,
        is_buy=False,
        price=100.0,
        qty=0.1,
        reduce_only=False,
        tif="Alo",
        created_ts=0.0,
        oid=77,
    )
    broker.orders_by_role["ask"] = order
    broker._oid_to_role[77] = "ask"

    asyncio.run(broker._sync_open_orders())
    assert "ask" not in broker.orders_by_role
    assert 77 in broker._retired_orders_by_oid

    market.delayed_fills = [
        {
            "coin": "io:SNDK",
            "oid": 77,
            "tid": "late-fill-1",
            "side": "A",
            "px": "100.0",
            "sz": "0.1",
            "fee": "0.001",
            "closedPnl": "0",
            "crossed": False,
            "time": 1000,
        }
    ]
    asyncio.run(broker._sync_fills())

    fills = broker.drain_fills()
    assert len(fills) == 1
    assert fills[0].order_role == "ask"
    assert fills[0].cloid == order.cloid
    assert broker.account.position_qty == pytest.approx(-0.1)


def test_filled_order_uses_recent_fill_fallback_after_sixty_seconds():
    cfg, meta, fees, _book = objects()

    class FakeMarket:
        async def open_orders(self, _address):
            return []

        async def order_status(self, _address, _cloid):
            return {
                "status": "order",
                "order": {
                    "status": "filled",
                    "order": {"origSz": "0.1", "sz": "0"},
                },
            }

        async def recent_fills(self, _address):
            return [
                {
                    "coin": "io:SNDK",
                    "oid": 88,
                    "tid": "recent-fallback-fill",
                    "side": "B",
                    "px": "99.0",
                    "sz": "0.1",
                    "fee": "0.001",
                    "closedPnl": "0",
                    "crossed": False,
                    "time": 1000,
                }
            ]

    broker = LiveBroker(
        cfg,
        meta,
        fees,
        FakeMarket(),  # type: ignore[arg-type]
        "not-used-in-this-test",
        "0x" + "1" * 40,
    )
    order = TrackedOrder(
        role="bid",
        cloid="0x" + "8" * 32,
        is_buy=True,
        price=99.0,
        qty=0.1,
        reduce_only=False,
        tif="Alo",
        created_ts=0.0,
        oid=88,
    )
    broker.orders_by_role["bid"] = order
    broker._oid_to_role[88] = "bid"
    broker._missing_since[order.cloid] = time.monotonic() - 61.0

    asyncio.run(broker._sync_open_orders())

    fills = broker.drain_fills()
    assert len(fills) == 1
    assert fills[0].fill_id == "recent-fallback-fill"
    assert "bid" not in broker.orders_by_role
    assert broker.account.position_qty == pytest.approx(0.1)


def test_fill_cursor_is_advanced_only_by_the_brokers_own_market():
    cfg, meta, fees, _book = objects()

    class FakeMarket:
        async def fills_since(self, _address, _start_ms):
            return [
                {
                    "coin": "io:SNDK",
                    "oid": 88,
                    "tid": "sndk-fill",
                    "side": "B",
                    "px": "99.0",
                    "sz": "0.1",
                    "fee": "0.001",
                    "closedPnl": "0",
                    "crossed": False,
                    "time": 1_000,
                },
                {
                    "coin": "io:ANTH",
                    "oid": 99,
                    "tid": "newer-other-market-fill",
                    "side": "A",
                    "px": "2000",
                    "sz": "0.1",
                    "fee": "0.001",
                    "closedPnl": "0",
                    "crossed": False,
                    "time": 50_000,
                },
            ]

    broker = LiveBroker(
        cfg,
        meta,
        fees,
        FakeMarket(),  # type: ignore[arg-type]
        "not-used-in-this-test",
        "0x" + "1" * 40,
    )
    broker._last_fill_ms = 0
    order = TrackedOrder(
        role="bid",
        cloid="0x" + "8" * 32,
        is_buy=True,
        price=99.0,
        qty=0.1,
        reduce_only=False,
        tif="Alo",
        created_ts=0.0,
        oid=88,
    )
    broker.orders_by_role["bid"] = order
    broker._oid_to_role[88] = "bid"

    asyncio.run(broker._sync_fills())

    assert broker._last_fill_ms == 1_001
    assert [fill.fill_id for fill in broker.drain_fills()] == ["sndk-fill"]


def test_filled_order_uses_order_time_deep_fallback_before_hard_stop():
    cfg, meta, fees, _book = objects()
    created_ts = time.time() - 300.0

    class FakeMarket:
        deep_start_ms = None

        async def open_orders(self, _address):
            return []

        async def order_status(self, _address, _cloid):
            return {
                "status": "order",
                "order": {
                    "status": "filled",
                    "order": {"origSz": "0.1", "sz": "0"},
                },
            }

        async def recent_fills(self, _address):
            return []

        async def fills_since(self, _address, start_ms):
            self.deep_start_ms = start_ms
            return [
                {
                    "coin": "io:SNDK",
                    "oid": 88,
                    "tid": "deep-recovered-fill",
                    "side": "B",
                    "px": "99.0",
                    "sz": "0.1",
                    "fee": "0.001",
                    "closedPnl": "0",
                    "crossed": False,
                    "time": int(created_ts * 1000) + 1_000,
                }
            ]

    market = FakeMarket()
    broker = LiveBroker(
        cfg,
        meta,
        fees,
        market,  # type: ignore[arg-type]
        "not-used-in-this-test",
        "0x" + "1" * 40,
    )
    order = TrackedOrder(
        role="bid",
        cloid="0x" + "8" * 32,
        is_buy=True,
        price=99.0,
        qty=0.1,
        reduce_only=False,
        tif="Alo",
        created_ts=created_ts,
        oid=88,
    )
    broker.orders_by_role["bid"] = order
    broker._oid_to_role[88] = "bid"
    broker._missing_since[order.cloid] = time.monotonic() - 241.0

    asyncio.run(broker._sync_open_orders())

    fills = broker.drain_fills()
    assert [fill.fill_id for fill in fills] == ["deep-recovered-fill"]
    assert market.deep_start_ms == max(int(created_ts * 1000) - 60_000, 0)
    assert "bid" not in broker.orders_by_role


def test_reduce_only_terminal_fill_accepts_exchange_clipped_remainder():
    cfg, meta, fees, _book = objects()

    class FakeMarket:
        async def open_orders(self, _address):
            return []

        async def order_status(self, _address, _cloid):
            return {
                "status": "order",
                "order": {
                    "status": "filled",
                    "order": {"origSz": "0.8", "sz": "0.1"},
                },
            }

    broker = LiveBroker(
        cfg,
        meta,
        fees,
        FakeMarket(),  # type: ignore[arg-type]
        "not-used-in-this-test",
        "0x" + "1" * 40,
    )
    order = TrackedOrder(
        role="rebalance_bid",
        cloid="0x" + "9" * 32,
        is_buy=True,
        price=99.0,
        qty=0.8,
        reduce_only=True,
        tif="Alo",
        created_ts=0.0,
        oid=90,
        filled_qty=0.7,
    )
    broker.orders_by_role[order.role] = order
    broker._oid_to_role[90] = order.role

    asyncio.run(broker._sync_open_orders())

    assert order.role not in broker.orders_by_role
    assert 90 in broker._retired_orders_by_oid


def test_non_reduce_only_terminal_fill_does_not_accept_short_local_total():
    cfg, meta, fees, _book = objects()

    class FakeMarket:
        async def open_orders(self, _address):
            return []

        async def order_status(self, _address, _cloid):
            return {
                "status": "order",
                "order": {
                    "status": "filled",
                    "order": {"origSz": "0.8", "sz": "0.1"},
                },
            }

    broker = LiveBroker(
        cfg,
        meta,
        fees,
        FakeMarket(),  # type: ignore[arg-type]
        "not-used-in-this-test",
        "0x" + "1" * 40,
    )
    order = TrackedOrder(
        role="bid",
        cloid="0x" + "a" * 32,
        is_buy=True,
        price=99.0,
        qty=0.8,
        reduce_only=False,
        tif="Alo",
        created_ts=0.0,
        oid=91,
        filled_qty=0.7,
    )
    broker.orders_by_role[order.role] = order
    broker._oid_to_role[91] = order.role

    with pytest.raises(PendingFillReconciliation):
        asyncio.run(broker._sync_open_orders())

    assert order.role in broker.orders_by_role


def test_reduce_only_terminal_fill_keeps_waiting_for_missing_local_detail():
    cfg, meta, fees, _book = objects()

    class FakeMarket:
        async def open_orders(self, _address):
            return []

        async def order_status(self, _address, _cloid):
            return {
                "status": "order",
                "order": {
                    "status": "filled",
                    "order": {"origSz": "0.8", "sz": "0.1"},
                },
            }

    broker = LiveBroker(
        cfg,
        meta,
        fees,
        FakeMarket(),  # type: ignore[arg-type]
        "not-used-in-this-test",
        "0x" + "1" * 40,
    )
    order = TrackedOrder(
        role="rebalance_bid",
        cloid="0x" + "b" * 32,
        is_buy=True,
        price=99.0,
        qty=0.8,
        reduce_only=True,
        tif="Alo",
        created_ts=0.0,
        oid=92,
        filled_qty=0.6,
    )
    broker.orders_by_role[order.role] = order
    broker._oid_to_role[92] = order.role

    with pytest.raises(PendingFillReconciliation):
        asyncio.run(broker._sync_open_orders())

    assert order.role in broker.orders_by_role


def test_cancel_path_releases_clipped_reduce_only_after_fallback_catches_up():
    cfg, meta, fees, _book = objects()

    class FakeMarket:
        async def order_status(self, _address, _cloid):
            return {
                "status": "order",
                "order": {
                    "status": "filled",
                    "order": {"origSz": "0.8", "sz": "0.1"},
                },
            }

        async def recent_fills(self, _address):
            return [
                {
                    "coin": "io:SNDK",
                    "oid": 93,
                    "tid": "clipped-final-fragment",
                    "side": "B",
                    "px": "99.0",
                    "sz": "0.1",
                    "fee": "0.001",
                    "closedPnl": "0",
                    "crossed": False,
                    "time": 1000,
                }
            ]

    broker = LiveBroker(
        cfg,
        meta,
        fees,
        FakeMarket(),  # type: ignore[arg-type]
        "not-used-in-this-test",
        "0x" + "1" * 40,
    )
    order = TrackedOrder(
        role="rebalance_bid",
        cloid="0x" + "c" * 32,
        is_buy=True,
        price=99.0,
        qty=0.8,
        reduce_only=True,
        tif="Alo",
        created_ts=0.0,
        oid=93,
        filled_qty=0.6,
    )
    broker.orders_by_role[order.role] = order
    broker._oid_to_role[93] = order.role
    broker._missing_since[order.cloid] = time.monotonic() - 61.0

    async def fake_cancel(_cloids):
        return None

    broker._cancel_cloids = fake_cancel  # type: ignore[method-assign]

    asyncio.run(broker.cancel_roles([order.role]))

    assert order.role not in broker.orders_by_role
    assert order.filled_qty == pytest.approx(0.7)
    assert 93 in broker._retired_orders_by_oid


class _IocExchange:
    def bulk_orders(self, *_args):
        raise AssertionError("tests replace LiveBroker._sdk")


def _ioc_broker(market=object()):
    cfg, meta, fees, book = objects()
    broker = LiveBroker(
        cfg,
        meta,
        fees,
        market,  # type: ignore[arg-type]
        "not-used-in-this-test",
        "0x" + "1" * 40,
    )
    broker.exchange = _IocExchange()
    broker.account.position_qty = 1.0
    return broker, book


def _filled_response(oid: int, qty: str = "0.5") -> dict:
    return {
        "status": "ok",
        "response": {
            "data": {
                "statuses": [
                    {"filled": {"oid": oid, "totalSz": qty, "avgPx": "99.8"}}
                ]
            }
        },
    }


def test_inventory_ioc_registers_oid_and_books_delayed_taker_fill():
    broker, book = _ioc_broker()

    async def fake_sdk(*_args):
        return _filled_response(501)

    broker._sdk = fake_sdk  # type: ignore[method-assign]
    asyncio.run(broker.reduce_position_ioc(book, 0.5, 15.0))

    assert broker._oid_to_role[501] == "risk:inventory-ioc"
    assert broker._retired_orders_by_oid[501][0].cloid
    events = broker.drain_order_events()
    action = next(row for row in events if row["event"] == "inventory_ioc")
    identity = next(row for row in events if row["event"] == "ioc_identity")
    assert action["outcome"] == "filled"
    assert action["oid"] == 501
    assert identity["role"] == "risk:inventory-ioc"

    broker._ingest_fill_row(
        {
            "coin": "io:SNDK",
            "oid": 501,
            "tid": "inventory-fill",
            "side": "A",
            "px": "99.8",
            "sz": "0.5",
            "fee": "0.0045",
            "feeToken": "USDC",
            "closedPnl": "-0.1",
            "crossed": True,
            "time": 123,
        }
    )
    fills = broker.drain_fills()
    assert len(fills) == 1
    assert fills[0].order_role == "risk:inventory-ioc"
    assert fills[0].liquidity == "taker"
    assert broker.account.position_qty == pytest.approx(0.5)
    assert broker.account.volume_usd == pytest.approx(49.9)
    assert broker.account.fees_paid == pytest.approx(0.0045)
    assert broker.account.realized_pnl == 0.0  # authoritative field updates on account sync
    assert broker._session_realized == pytest.approx(-0.1)


@pytest.mark.parametrize("method_name", ["inventory", "flatten"])
def test_reduce_only_ioc_subitem_error_never_returns_success(method_name):
    broker, book = _ioc_broker()

    async def fake_sdk(*_args):
        return {
            "status": "ok",
            "response": {
                "data": {"statuses": [{"error": "could not immediately match"}]}
            },
        }

    broker._sdk = fake_sdk  # type: ignore[method-assign]
    call = (
        broker.reduce_position_ioc(book, 0.5, 15.0)
        if method_name == "inventory"
        else broker.flatten(book, 15.0)
    )
    with pytest.raises(RuntimeError, match="被交易所拒绝"):
        asyncio.run(call)
    actions = [
        row
        for row in broker.drain_order_events()
        if row["event"] in {"inventory_ioc", "flatten"}
    ]
    assert len(actions) == 1
    assert actions[0]["outcome"] == "rejected"
    assert broker._oid_to_role == {}


def test_uncertain_ioc_send_is_reconciled_by_cloid_without_resubmit():
    class Market:
        calls = 0
        actual_cloid = ""

        async def order_status(self, _address, lookup):
            self.calls += 1
            if isinstance(lookup, str) and is_bot_cloid(lookup):
                self.actual_cloid = lookup
            assert self.actual_cloid
            return {
                "status": "order",
                "order": {
                    "status": "filled",
                    "oid": 601,
                    "order": {"oid": 601, "cloid": self.actual_cloid},
                },
            }

    market = Market()
    broker, book = _ioc_broker(market)
    sdk_calls = 0

    async def timeout_after_send(*_args):
        nonlocal sdk_calls
        sdk_calls += 1
        raise TimeoutError("response lost")

    broker._sdk = timeout_after_send  # type: ignore[method-assign]
    asyncio.run(broker.reduce_position_ioc(book, 0.5, 15.0))

    assert sdk_calls == 1
    # A cloid hit that reveals an oid is verified through both identities
    # before a non-idempotent result is released.
    assert market.calls == 2
    assert broker._oid_to_role[601] == "risk:inventory-ioc"
    action = next(
        row for row in broker.drain_order_events() if row["event"] == "inventory_ioc"
    )
    assert action["outcome"] == "reconciled_filled"


@pytest.mark.parametrize(
    "malformed",
    [
        None,
        {},
        {"status": "err"},
        {"status": "mystery"},
        {"status": "err", "response": {"data": "ambiguous"}},
    ],
)
def test_malformed_ioc_top_response_reconciles_accepted_cloid_once(malformed):
    class Market:
        calls = 0
        actual_cloid = ""

        async def order_status(self, _address, lookup):
            self.calls += 1
            if isinstance(lookup, str) and is_bot_cloid(lookup):
                self.actual_cloid = lookup
            assert self.actual_cloid
            return {
                "status": "order",
                "order": {
                    "status": "filled",
                    "oid": 602,
                    "order": {"oid": 602, "cloid": self.actual_cloid},
                },
            }

    market = Market()
    broker, book = _ioc_broker(market)
    sdk_calls = 0

    async def malformed_after_send(*_args):
        nonlocal sdk_calls
        sdk_calls += 1
        return malformed

    broker._sdk = malformed_after_send  # type: ignore[method-assign]
    asyncio.run(broker.reduce_position_ioc(book, 0.5, 15.0))

    assert sdk_calls == 1
    assert market.calls == 2
    assert broker._oid_to_role[602] == "risk:inventory-ioc"


@pytest.mark.parametrize(
    "detail",
    ["insufficient margin", {"error": "insufficient margin"}],
)
def test_explicit_ioc_top_error_is_rejection_without_probe(detail):
    class Market:
        async def order_status(self, *_args):
            raise AssertionError("an explicit exchange rejection needs no probe")

    broker, book = _ioc_broker(Market())

    async def fake_sdk(*_args):
        return {"status": "err", "response": detail}

    broker._sdk = fake_sdk  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="被交易所拒绝"):
        asyncio.run(broker.reduce_position_ioc(book, 0.5, 15.0))


def test_known_ioc_oid_cannot_regress_to_not_found_or_resubmit(monkeypatch):
    class Market:
        def __init__(self):
            self.lookups = []

        async def order_status(self, _address, lookup):
            self.lookups.append(lookup)
            return {"status": "unknownOid"}

    async def no_sleep(_seconds):
        return None

    monkeypatch.setattr("entropy_bot.broker.asyncio.sleep", no_sleep)
    market = Market()
    broker, book = _ioc_broker(market)
    sdk_calls = 0

    async def resting_response(*_args):
        nonlocal sdk_calls
        sdk_calls += 1
        return {
            "status": "ok",
            "response": {"data": {"statuses": [{"resting": {"oid": 702}}]}},
        }

    async def cancel_once(_cloids):
        return None

    broker._sdk = resting_response  # type: ignore[method-assign]
    broker._cancel_cloids = cancel_once  # type: ignore[method-assign]
    with pytest.raises(UncertainIocOutcome, match="禁止重发"):
        asyncio.run(broker.reduce_position_ioc(book, 0.5, 15.0))
    with pytest.raises(UncertainIocOutcome, match="禁止重发"):
        asyncio.run(broker.reduce_position_ioc(book, 0.5, 15.0))

    assert sdk_calls == 1
    assert 702 in market.lookups
    assert len(broker._pending_ioc_orders) == 1


def test_unknown_oid_never_releases_uncertain_ioc_for_later_resubmit(monkeypatch):
    class Market:
        def __init__(self):
            self.lookups = []

        async def order_status(self, _address, lookup):
            self.lookups.append(lookup)
            return {"status": "unknownOid"}

    async def no_sleep(_seconds):
        return None

    monkeypatch.setattr("entropy_bot.broker.asyncio.sleep", no_sleep)
    market = Market()
    broker, book = _ioc_broker(market)
    sdk_calls = 0

    async def timeout_after_send(*_args):
        nonlocal sdk_calls
        sdk_calls += 1
        raise TimeoutError("response lost")

    broker._sdk = timeout_after_send  # type: ignore[method-assign]
    for _ in range(3):
        with pytest.raises(UncertainIocOutcome, match="禁止重发"):
            asyncio.run(broker.reduce_position_ioc(book, 0.5, 15.0))

    assert sdk_calls == 1
    assert len(broker._pending_ioc_orders) == 1
    assert len(market.lookups) == 6


def test_known_ioc_oid_conflicting_with_cloid_view_stays_pending():
    broker, _book = _ioc_broker()
    cloid = broker.cloids.next()

    class Market:
        def __init__(self):
            self.lookups = []

        async def order_status(self, _address, lookup):
            self.lookups.append(lookup)
            state = "filled" if isinstance(lookup, int) else "canceled"
            return {
                "status": "order",
                "order": {
                    "status": state,
                    "oid": 704,
                    "order": {"oid": 704, "cloid": cloid},
                },
            }

    market = Market()
    broker.market = market
    order = TrackedOrder(
        role="risk:inventory-ioc",
        cloid=cloid,
        is_buy=False,
        price=99.75,
        qty=0.5,
        reduce_only=True,
        tif="Ioc",
        created_ts=time.time(),
        oid=704,
    )
    broker._pending_ioc_orders[cloid] = order

    with pytest.raises(UncertainIocOutcome, match="禁止重发"):
        asyncio.run(broker._resolve_ioc_after_uncertain_send(order))

    assert market.lookups == [704, cloid]
    assert broker._pending_ioc_orders[cloid] is order


def test_known_ioc_oid_terminal_view_can_survive_lagging_cloid_index():
    broker, _book = _ioc_broker()
    cloid = broker.cloids.next()

    class Market:
        def __init__(self):
            self.lookups = []

        async def order_status(self, _address, lookup):
            self.lookups.append(lookup)
            if isinstance(lookup, str):
                return {"status": "unknownOid"}
            return {
                "status": "order",
                "order": {
                    "status": "filled",
                    "oid": 705,
                    "order": {"oid": 705, "cloid": cloid},
                },
            }

    market = Market()
    broker.market = market
    order = TrackedOrder(
        role="risk:inventory-ioc",
        cloid=cloid,
        is_buy=False,
        price=99.75,
        qty=0.5,
        reduce_only=True,
        tif="Ioc",
        created_ts=time.time(),
        oid=705,
    )
    broker._pending_ioc_orders[cloid] = order

    assert asyncio.run(broker._resolve_ioc_after_uncertain_send(order)) == "filled"
    assert market.lookups == [705, cloid]
    assert broker._pending_ioc_orders == {}


def test_filled_without_oid_stays_pending_after_unknown_status(monkeypatch):
    class Market:
        async def order_status(self, _address, _lookup):
            return {"status": "unknownOid"}

    async def no_sleep(_seconds):
        return None

    monkeypatch.setattr("entropy_bot.broker.asyncio.sleep", no_sleep)
    broker, book = _ioc_broker(Market())
    sdk_calls = 0

    async def filled_without_oid(*_args):
        nonlocal sdk_calls
        sdk_calls += 1
        return {
            "status": "ok",
            "response": {
                "data": {"statuses": [{"filled": {"totalSz": "0.5"}}]}
            },
        }

    broker._sdk = filled_without_oid  # type: ignore[method-assign]
    with pytest.raises(UncertainIocOutcome, match="禁止重发"):
        asyncio.run(broker.reduce_position_ioc(book, 0.5, 15.0))
    with pytest.raises(UncertainIocOutcome, match="禁止重发"):
        asyncio.run(broker.reduce_position_ioc(book, 0.5, 15.0))

    assert sdk_calls == 1
    assert len(broker._pending_ioc_orders) == 1


def test_contradictory_ioc_subitem_is_reconciled_not_treated_as_rejection():
    class Market:
        actual_cloid = ""

        async def order_status(self, _address, lookup):
            if isinstance(lookup, str) and is_bot_cloid(lookup):
                self.actual_cloid = lookup
            return {
                "status": "order",
                "order": {
                    "status": "filled",
                    "oid": 703,
                    "order": {"oid": 703, "cloid": self.actual_cloid},
                },
            }

    broker, book = _ioc_broker(Market())
    sdk_calls = 0

    async def contradictory(*_args):
        nonlocal sdk_calls
        sdk_calls += 1
        return {
            "status": "ok",
            "response": {
                "data": {
                    "statuses": [
                        {"error": "ambiguous", "filled": {"oid": 703}}
                    ]
                }
            },
        }

    broker._sdk = contradictory  # type: ignore[method-assign]
    asyncio.run(broker.reduce_position_ioc(book, 0.5, 15.0))
    assert sdk_calls == 1
    assert broker._oid_to_role[703] == "risk:inventory-ioc"


def test_unresolved_ioc_cloid_blocks_every_later_ioc_before_send():
    class Market:
        async def order_status(self, _address, _cloid):
            raise TimeoutError("read unavailable")

    broker, book = _ioc_broker(Market())
    sdk_calls = 0

    async def timeout_after_send(*_args):
        nonlocal sdk_calls
        sdk_calls += 1
        raise TimeoutError("response lost")

    broker._sdk = timeout_after_send  # type: ignore[method-assign]
    with pytest.raises(UncertainIocOutcome, match="禁止重发"):
        asyncio.run(broker.reduce_position_ioc(book, 0.5, 15.0))
    assert len(broker._pending_ioc_orders) == 1

    # The second call performs only a read-only cloid reconciliation and is
    # rejected before a new signed action reaches _sdk.
    with pytest.raises(UncertainIocOutcome, match="禁止重发"):
        asyncio.run(broker.reduce_position_ioc(book, 0.5, 15.0))
    assert sdk_calls == 1


def test_abnormal_resting_ioc_is_canceled_and_reconciled():
    class Market:
        async def order_status(self, _address, lookup):
            return {
                "status": "order",
                "order": {
                    "status": "canceled",
                    "oid": 701,
                    "order": {
                        "oid": 701,
                        "cloid": lookup if isinstance(lookup, str) else "",
                    },
                },
            }

    broker, book = _ioc_broker(Market())

    async def fake_sdk(*_args):
        return {
            "status": "ok",
            "response": {"data": {"statuses": [{"resting": {"oid": 701}}]}},
        }

    canceled = []

    async def fake_cancel(cloids):
        canceled.extend(cloids)

    broker._sdk = fake_sdk  # type: ignore[method-assign]
    broker._cancel_cloids = fake_cancel  # type: ignore[method-assign]
    asyncio.run(broker.reduce_position_ioc(book, 0.5, 15.0))

    order = broker._retired_orders_by_oid[701][0]
    assert canceled == [order.cloid]
    assert broker._pending_ioc_orders == {}
    action = next(
        row for row in broker.drain_order_events() if row["event"] == "inventory_ioc"
    )
    assert action["outcome"] == "resting_then_canceled"


def test_flatten_registers_fill_and_requires_official_zero_position():
    class Market:
        def __init__(self, residual: float):
            self.residual = residual

        async def fills_since(self, _address, _start_ms):
            return [
                {
                    "coin": "io:SNDK",
                    "oid": 801,
                    "tid": "flatten-fill",
                    "side": "A",
                    "px": "99.8",
                    "sz": "0.6" if self.residual else "1.0",
                    "fee": "0.009",
                    "closedPnl": "-0.2",
                    "crossed": True,
                    "time": 456,
                }
            ]

        async def account_state(self, _address):
            positions = []
            if self.residual:
                positions = [
                    {
                        "position": {
                            "coin": "io:SNDK",
                            "szi": str(self.residual),
                            "entryPx": "100",
                            "unrealizedPnl": "0",
                        }
                    }
                ]
            return {"assetPositions": positions, "time": int(time.time() * 1000)}

    async def run(residual):
        broker, book = _ioc_broker(Market(residual))

        async def fake_sdk(*_args):
            return _filled_response(801, "0.6" if residual else "1.0")

        broker._sdk = fake_sdk  # type: ignore[method-assign]
        if residual:
            with pytest.raises(RuntimeError, match="官方仍有剩余仓位"):
                await broker.flatten(book, 15.0)
        else:
            await broker.flatten(book, 15.0)
        return broker

    flat = asyncio.run(run(0.0))
    assert flat.account.position_qty == 0.0
    fills = flat.drain_fills()
    assert len(fills) == 1
    assert fills[0].order_role == "risk:emergency-flatten"
    assert flat._oid_to_role[801] == "risk:emergency-flatten"

    partial = asyncio.run(run(0.4))
    assert partial.account.position_qty == pytest.approx(0.4)
    assert partial._oid_to_role[801] == "risk:emergency-flatten"


@pytest.mark.parametrize("status", [
    "tickRejected", "minTradeNtlRejected", "perpMarginRejected",
    "reduceOnlyRejected", "badAloPxRejected", "iocCancelRejected",
    "badTriggerPxRejected", "marketOrderNoLiquidityRejected",
    "positionIncreaseAtOpenInterestCapRejected", "positionFlipAtOpenInterestCapRejected",
    "tooAggressiveAtOpenInterestCapRejected", "openInterestIncreaseRejected",
    "insufficientSpotBalanceRejected", "oracleRejected", "perpMaxPositionRejected",
])
def test_official_rejection_subtypes_retire_without_uncertain_wait(status):
    cfg, meta, fees, _book = objects()

    class Market:
        async def order_status(self, _address, cloid):
            return {"status": "order", "order": {
                "status": status,
                "order": {"oid": 901, "cloid": cloid, "origSz": "1", "sz": "1"},
            }}

    broker = LiveBroker(cfg, meta, fees, Market(), "unused", "0x" + "1" * 40)
    order = TrackedOrder("grid:buy", broker.cloids.next(), True, 99.0, 1.0,
                         False, "Alo", time.time(), oid=901)
    broker.orders_by_role[order.role] = order
    broker._oid_to_role[901] = order.role
    asyncio.run(broker._reconcile_missing_tracked(set(), set()))
    assert not broker.orders_by_role
    assert 901 in broker._retired_orders_by_oid


@pytest.mark.parametrize(("field", "value", "fee_known", "pnl_known"), [
    (None, None, True, True),
    ("fee", None, False, True),
    ("fee", "NaN", False, True),
    ("feeToken", None, False, True),
    ("feeToken", "HYPE", False, True),
    ("closedPnl", None, True, False),
    ("closedPnl", "Infinity", True, False),
])
def test_live_fill_preserves_cost_evidence_without_inventing_missing_values(
    field, value, fee_known, pnl_known
):
    cfg, meta, fees, _book = objects()
    broker = LiveBroker(cfg, meta, fees, object(), "unused", "0x" + "1" * 40)
    order = TrackedOrder("grid:buy", broker.cloids.next(), True, 99.0, 2.0,
                         False, "Alo", time.time(), oid=902)
    broker.orders_by_role[order.role] = order
    broker._oid_to_role[902] = order.role
    row = {"coin": meta.coin, "time": 1000, "oid": 902, "tid": "fill-902",
           "side": "B", "px": "99", "sz": "1", "fee": "0.03",
           "feeToken": " USDC ", "builderFee": "0.01", "closedPnl": "-5",
           "startPosition": "-2.5", "crossed": False}
    if field:
        if value is None:
            row.pop(field)
        else:
            row[field] = value
    broker._ingest_fill_row(row)
    fill, = broker.drain_fills()
    assert fill.start_position_qty == -2.5
    assert fill.fee_known is fee_known
    assert fill.realized_pnl_known is pnl_known
    assert fill.fee_usd == (0.03 if fee_known else 0.0)
    assert fill.realized_pnl_delta == (-5.0 if pnl_known else 0.0)
    assert broker.account.fee_accounting_complete is fee_known
    assert broker.account.realized_accounting_complete is pnl_known
    if field == "feeToken" and value is None:
        assert fill.fee_token is None
    # A later complete fill cannot repair missing evidence for the previous
    # one; session totals must keep their incomplete marker.
    row.update(tid="fill-903", fee="0.03", feeToken="USDC", closedPnl="0")
    broker._ingest_fill_row(row)
    assert broker.account.fee_accounting_complete is fee_known
    assert broker.account.realized_accounting_complete is pnl_known


def test_paper_fill_ids_survive_process_restart_and_keep_pre_fill_inventory():
    cfg, meta, fees, _book = objects()
    fills = []
    for _ in range(2):
        broker = PaperBroker(cfg, meta, fees)
        broker.account.position_qty = 2.5
        broker.account.entry_px = 100.0
        order = TrackedOrder("grid:-1", broker.cloids.next(), True, 99.0, 1.0,
                             False, "Alo", time.time())
        broker.orders_by_role[order.role] = order
        broker._fill(order, 99.0)
        fill, = broker.drain_fills()
        fills.append(fill)
        assert fill.start_position_qty == 2.5
        assert broker.account.position_qty == 3.5
        assert order.cloid in fill.fill_id
    assert fills[0].fill_id != fills[1].fill_id


@pytest.mark.parametrize("action", ["place", "modify", "ioc"])
@pytest.mark.parametrize("journal_fails", [False, True])
def test_grid_intent_is_durable_before_send_or_failed_journal_blocks_send(
    tmp_path, action, journal_fails
):
    import json
    import os

    class SimulatedProcessExit(BaseException):
        pass

    broker, book = _ioc_broker()
    broker.exchange.bulk_modify_orders_new = object()
    old = TrackedOrder("grid:-1", broker.cloids.next(), True, 99, 1,
                       False, "Alo", time.time(), oid=700)
    if action == "modify":
        broker.orders_by_role[old.role] = old
        broker._oid_to_role[700] = old.role
    path = tmp_path / "durable-intents.jsonl"

    def journal(row):
        if journal_fails:
            raise OSError("journal unavailable")
        with path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(row) + "\n")
            stream.flush()
            os.fsync(stream.fileno())

    broker.order_intent_sink = journal
    calls = []

    async def fake_sdk(_fn, requests):
        saved = json.loads(path.read_text(encoding="utf-8").splitlines()[0])
        request = requests[0]["order"] if action == "modify" else requests[0]
        assert saved["cloid"] == request["cloid"].to_raw()
        assert saved["role"].startswith("grid:")
        assert saved["outcome"] == "submitted"
        if action == "modify":
            assert saved["oid"] == 700
            assert saved["previous_cloid"] == old.cloid
        calls.append(saved)
        # An abrupt process exit after network dispatch bypasses normal
        # outcome logging; the pre-send identity must still be on disk.
        raise SimulatedProcessExit()

    broker._sdk = fake_sdk

    async def run():
        intent = OrderIntent("grid:-1", True, 99.5, 1)
        if action == "place":
            await broker.place([intent], book)
        elif action == "modify":
            await broker.modify_orders([(old.role, intent)], book)
        else:
            await broker._submit_reduce_only_ioc(
                role="grid:close", event="inventory_ioc", is_buy=False, qty=0.5, px=99.8
            )

    expected = OSError if journal_fails else SimulatedProcessExit
    with pytest.raises(expected):
        asyncio.run(run())
    assert len(calls) == (0 if journal_fails else 1)
    assert path.exists() is (not journal_fails)


def test_paper_journal_failure_prevents_order_creation():
    cfg, meta, fees, book = objects()
    broker = PaperBroker(cfg, meta, fees)

    def fail(_row):
        raise OSError("journal unavailable")

    broker.order_intent_sink = fail
    with pytest.raises(OSError):
        asyncio.run(broker.place([OrderIntent("grid:-1", True, 99, 1)], book))
    assert broker.orders_by_role == {}
