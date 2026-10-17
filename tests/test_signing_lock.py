"""Real process boundaries and cancellation, using only synthetic identities."""
from __future__ import annotations

import asyncio
import errno
import json
import multiprocessing
import threading
import time
from dataclasses import replace
from pathlib import Path

import pytest

from entropy_bot.broker import LiveBroker
from entropy_bot.broker_protocol import PaidActionRecoveryUnavailable
from entropy_bot.signing_lock import (
    SignerProcessLock, SigningIdentityUnavailable, SigningInterrupted, SigningLockError, SigningLockTimeout,
)

SIGNER = "0x" + "a" * 40


def _process_call(directory, signer, events, release=None):
    lock = SignerProcessLock(signer, directory=Path(directory), timeout=5)

    def body():
        events.put(("entered", time.monotonic_ns()))
        if release is not None:
            if not release.wait(8):
                raise RuntimeError("test release timed out")
        else:
            time.sleep(0.08)
        events.put(("left", time.monotonic_ns()))

    lock.run(body)


def test_signer_domain_is_case_insensitive_and_does_not_leak_address(tmp_path):
    lock = SignerProcessLock(SIGNER, directory=tmp_path)
    assert lock.path == SignerProcessLock(SIGNER.upper().replace("0X", "0x"), directory=tmp_path).path
    assert SIGNER not in str(lock.path)
    assert len(lock.path.stem) == 64
    assert lock.path != SignerProcessLock("0x" + "b" * 40, directory=tmp_path).path


def test_real_processes_serialize_same_signer(tmp_path):
    ctx = multiprocessing.get_context("spawn")
    events = ctx.Queue()
    processes = [ctx.Process(target=_process_call, args=(str(tmp_path), SIGNER, events)) for _ in range(3)]
    try:
        for process in processes:
            process.start()
        for process in processes:
            process.join(12)
            assert process.exitcode == 0
        rows = sorted([events.get(timeout=2) for _ in range(6)], key=lambda row: row[1])
        assert [row[0] for row in rows] == ["entered", "left"] * 3
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(3)
        events.close()


def test_lock_timeout_and_killed_owner_release_os_lock_but_preserve_signer_gate(tmp_path, monkeypatch):
    ctx = multiprocessing.get_context("spawn")
    events, release = ctx.Queue(), ctx.Event()
    owner = ctx.Process(target=_process_call, args=(str(tmp_path), SIGNER, events, release))
    owner.start()
    called = []
    try:
        assert events.get(timeout=8)[0] == "entered"
        with pytest.raises(SigningLockTimeout):
            SignerProcessLock(SIGNER, directory=tmp_path, timeout=0.08).run(lambda: called.append(True))
        assert called == []
        # A different API key has a distinct nonce domain and does not wait.
        assert SignerProcessLock("0x" + "b" * 40, directory=tmp_path).run(lambda: 19) == 19
        owner.terminate()
        owner.join(5)
        assert not owner.is_alive()
        lock = SignerProcessLock(SIGNER, directory=tmp_path, timeout=2)
        marker = lock.path.read_bytes()
        assert json.loads(marker)["state"] == "in_progress"
        # Even a wall clock rollback cannot enter the signing callback after
        # an interrupted owner. Reaching this error proves the OS lock released.
        from entropy_bot import signing_lock
        monkeypatch.setattr(signing_lock.time, "time", lambda: 1.0)
        with pytest.raises(SigningInterrupted):
            lock.run(lambda: pytest.fail("must not sign after killed owner"))
        assert lock.path.read_bytes() == marker
    finally:
        if owner.is_alive():
            owner.terminate()
            owner.join(3)
        events.close()


def test_failed_call_releases_lock_and_next_nonce_time_is_later(tmp_path):
    lock = SignerProcessLock(SIGNER, directory=tmp_path)
    timestamps = []

    def fail():
        timestamps.append(int(time.time() * 1000))
        raise RuntimeError("synthetic send failure")

    with pytest.raises(RuntimeError, match="synthetic"):
        lock.run(fail)
    lock.run(lambda: timestamps.append(int(time.time() * 1000)))
    assert timestamps[1] > timestamps[0]


@pytest.mark.parametrize("raw", [b"broken", b"-1", b"9999999999999"])
def test_invalid_or_future_timestamp_fails_closed(tmp_path, raw):
    lock = SignerProcessLock(SIGNER, directory=tmp_path, timeout=0.03)
    lock.path.write_bytes(raw)
    calls = []
    with pytest.raises(SigningLockError):
        lock.run(lambda: calls.append(True))
    assert not calls


def test_unavailable_directory_fails_closed(tmp_path):
    root = tmp_path / "file"
    root.write_text("cannot be a directory")
    with pytest.raises(SigningLockError):
        SignerProcessLock(SIGNER, directory=root).run(lambda: pytest.fail("must not sign"))


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan")])
def test_invalid_timeout_cannot_disable_lock_wait_bound(timeout, tmp_path):
    with pytest.raises(ValueError):
        SignerProcessLock(SIGNER, directory=tmp_path, timeout=timeout)


@pytest.mark.parametrize("signer", ["", "0x" + "g" * 40, "0x" + "a" * 39])
def test_missing_or_invalid_signer_cannot_select_unscoped_lock(signer, tmp_path):
    with pytest.raises(SigningIdentityUnavailable):
        SignerProcessLock(signer, directory=tmp_path)


def test_os_lock_failure_is_not_treated_as_contention_or_ignored(tmp_path, monkeypatch):
    def unavailable(_handle):
        raise OSError(errno.ENOSYS, "synthetic unsupported lock")

    monkeypatch.setattr(SignerProcessLock, "_try_acquire", staticmethod(unavailable))
    with pytest.raises(SigningLockError, match="无法取得"):
        SignerProcessLock(SIGNER, directory=tmp_path).run(lambda: pytest.fail("must not sign"))


def _broker(tmp_path):
    from test_broker import objects
    cfg, meta, fees, book = objects()
    broker = LiveBroker(cfg, meta, fees, object(), "synthetic-unused-key", "0x" + "1" * 40)
    broker._signing_lock = SignerProcessLock(SIGNER, directory=tmp_path)
    return broker, book


def test_cancelling_sdk_waits_for_thread_before_next_call(tmp_path):
    broker, _book = _broker(tmp_path)
    entered, release, second_entered = threading.Event(), threading.Event(), threading.Event()

    def first():
        entered.set()
        assert release.wait(5)

    async def scenario():
        task = asyncio.create_task(broker._sdk(first))
        assert await asyncio.to_thread(entered.wait, 3)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        following = asyncio.create_task(broker._sdk(second_entered.set))
        await asyncio.sleep(0.03)
        assert not task.done()
        assert not second_entered.is_set()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        await following
        assert second_entered.is_set()

    try:
        asyncio.run(scenario())
    finally:
        release.set()


@pytest.mark.parametrize("operation", ["place", "modify", "ioc"])
def test_cancelled_send_reserves_original_identity_and_durable_intent(tmp_path, operation):
    from entropy_bot.models import OrderIntent, TrackedOrder
    broker, book = _broker(tmp_path)
    entered, release = threading.Event(), threading.Event()
    journal = []
    broker.order_intent_sink = lambda row: journal.append(dict(row))

    class FakeExchange:
        def bulk_orders(self, _requests):
            entered.set()
            assert release.wait(5)
            return {"status": "ok", "response": {"data": {"statuses": [{"resting": {"oid": 101}}]}}}

        bulk_modify_orders_new = bulk_orders

    broker.exchange = FakeExchange()
    intent = OrderIntent("bid", True, 99, 1)
    if operation == "modify":
        old = TrackedOrder("bid", broker.cloids.next(), True, 98, 1, False, "Alo", 0, oid=99)
        broker.orders_by_role["bid"] = old
        broker._oid_to_role[99] = "bid"

    async def scenario():
        if operation == "place":
            call = broker.place([intent], book)
        elif operation == "modify":
            call = broker.modify_orders([("bid", intent)], book)
        else:
            call = broker._submit_reduce_only_ioc(role="risk:ioc", event="inventory_ioc", is_buy=False, qty=1, px=99)
        task = asyncio.create_task(call)
        assert await asyncio.to_thread(entered.wait, 3)
        assert len(journal) == 1  # Persisted before the synthetic signer entered.
        task.cancel()
        await asyncio.sleep(0)
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task

    try:
        asyncio.run(scenario())
    finally:
        release.set()
    if operation == "ioc":
        assert list(broker._pending_ioc_orders) == [journal[0]["cloid"]]
    else:
        reserved = broker.orders_by_role["bid"]
        assert reserved.cloid == journal[0]["cloid"]
        assert reserved.oid is None
        assert reserved.cloid in broker._identity_pending_cloids
    assert broker.drain_order_events()[-1]["outcome"] == "send_uncertain"


def test_uninitialized_signer_never_executes(tmp_path):
    broker, _book = _broker(tmp_path)
    broker._signing_lock = None
    with pytest.raises(SigningIdentityUnavailable):
        asyncio.run(broker._sdk(lambda: pytest.fail("must not sign")))


def test_paid_quota_is_blocked_at_start_and_direct_execution(tmp_path):
    broker, _book = _broker(tmp_path)
    broker.cfg = replace(broker.cfg, runtime=replace(broker.cfg.runtime, auto_reserve_actions=True))
    for call in (broker.start, broker._maybe_reserve_actions):
        with pytest.raises(PaidActionRecoveryUnavailable) as error:
            asyncio.run(call())
        assert error.value.code == "PAID_ACTION_RECOVERY_UNAVAILABLE"
    with pytest.raises(PaidActionRecoveryUnavailable):
        broker._reserve_weight_sync(100)
    assert broker.exchange is None


@pytest.mark.parametrize(("operation", "response_kind"), [
    ("place", "malformed"), ("modify", "malformed"), ("ioc", "malformed"),
    ("place", "partial"), ("modify", "partial"),
])
def test_cancellation_during_reconciliation_retains_all_unresolved_batch_identities(
    tmp_path, operation, response_kind,
):
    from entropy_bot.models import OrderIntent, TrackedOrder
    broker, book = _broker(tmp_path)
    journal = []
    broker.order_intent_sink = lambda row: journal.append(dict(row))
    intents = [OrderIntent(role, True, 99, 1) for role in ("bid:1", "bid:2", "bid:3")]
    if operation == "modify":
        for index, intent in enumerate(intents):
            old = TrackedOrder(intent.role, broker.cloids.next(), True, 98, 1, False, "Alo", 0, oid=900 + index)
            broker.orders_by_role[intent.role] = old
            broker._oid_to_role[old.oid] = old.role

    class FakeExchange:
        bulk_orders = object()
        bulk_modify_orders_new = object()

    broker.exchange = FakeExchange()

    async def synthetic_sdk(*_args):
        if response_kind == "partial" and operation != "ioc":
            return {"status": "ok", "response": {"data": {"statuses": [
                {"resting": {"oid": 801}}, {}, {},
            ]}}}
        return {"status": "ok", "response": {"data": {"statuses": []}}}

    broker._sdk = synthetic_sdk

    async def scenario():
        querying = asyncio.Event()

        class WaitingMarket:
            async def order_status(self, *_args):
                querying.set()
                await asyncio.Event().wait()

        broker.market = WaitingMarket()
        if operation == "place":
            call = broker.place(intents, book)
        elif operation == "modify":
            call = broker.modify_orders([(intent.role, intent) for intent in intents], book)
        else:
            call = broker._submit_reduce_only_ioc(role="risk:ioc", event="inventory_ioc", is_buy=False, qty=1, px=99)
        task = asyncio.create_task(call)
        await asyncio.wait_for(querying.wait(), timeout=2)
        # Every batch identity is already held while the first query awaits.
        assert set(broker._inflight_order_intents) == {row["cloid"] for row in journal}
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())
    assert broker._inflight_order_intents == {}
    if operation == "ioc":
        assert set(broker._pending_ioc_orders) == {journal[0]["cloid"]}
    else:
        start = 1 if response_kind == "partial" else 0
        pending = {row["cloid"] for row in journal[start:]}
        assert broker._identity_pending_cloids == pending
        assert all(broker.orders_by_role[row["role"]].oid is None for row in journal[start:])
        if response_kind == "partial":
            # Cancellation of another row must not erase already-proven IDs.
            assert broker.orders_by_role["bid:1"].oid == 801


def test_clock_rollback_during_call_leaves_interrupted_gate(tmp_path, monkeypatch):
    from entropy_bot import signing_lock
    lock = SignerProcessLock(SIGNER, directory=tmp_path)
    ticks = iter([10.0, 10.001, 10.001, 9.0])
    monkeypatch.setattr(signing_lock.time, "time", lambda: next(ticks))
    with pytest.raises(SigningInterrupted):
        lock.run(lambda: "synthetic result")
    assert json.loads(lock.path.read_bytes())["state"] == "in_progress"
    with pytest.raises(SigningInterrupted):
        lock.run(lambda: pytest.fail("must not sign"))


def test_pre_signing_marker_must_be_durable_before_sdk_is_called(tmp_path, monkeypatch):
    from entropy_bot import signing_lock
    lock = SignerProcessLock(SIGNER, directory=tmp_path)

    def fsync_denied(_fd):
        raise OSError("synthetic fsync failure")

    monkeypatch.setattr(signing_lock.os, "fsync", fsync_denied)
    with pytest.raises(OSError, match="fsync"):
        lock.run(lambda: pytest.fail("must not sign before durable marker"))


def test_short_marker_write_is_not_reported_as_durable():
    class ShortWrite:
        def seek(self, _offset):
            pass

        def write(self, payload):
            return len(payload) - 1

    with pytest.raises(SigningLockError, match="未完整写入"):
        SignerProcessLock._record(ShortWrite(), "in_progress", 123)


@pytest.mark.parametrize("phase", ["fills", "position"])
def test_cancelled_ioc_terminal_refresh_keeps_remaining_position_gate(tmp_path, phase):
    from entropy_bot.models import TrackedOrder
    broker, _book = _broker(tmp_path)
    order = TrackedOrder("risk:ioc", broker.cloids.next(), False, 99, 1, True, "Ioc", 0, oid=501)
    broker._pending_ioc_orders[order.cloid] = order

    async def resolved(item):
        broker._pending_ioc_orders.pop(item.cloid, None)
        return "filled"

    async def scenario():
        refreshing = asyncio.Event()

        async def fills(**_kwargs):
            if phase == "fills":
                refreshing.set()
                await asyncio.Event().wait()

        async def position():
            refreshing.set()
            await asyncio.Event().wait()

        broker._resolve_ioc_after_uncertain_send = resolved
        broker._sync_fills = fills
        broker._sync_position_only = position
        task = asyncio.create_task(broker._reconcile_pending_iocs())
        await asyncio.wait_for(refreshing.wait(), timeout=2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())
    assert broker._pending_ioc_orders == {order.cloid: order}
