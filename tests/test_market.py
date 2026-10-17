import asyncio
from pathlib import Path

import pytest

from entropy_bot.config import load_config
from entropy_bot.market import InfoRateLimiter, MarketClient, info_request_weight


def history_client(rows, *, page_size=500):
    cfg = load_config(Path(__file__).parents[1] / "config.market_maker.json")
    client = MarketClient(cfg.market, object())
    calls = []

    async def info(payload):
        calls.append(dict(payload))
        return [
            row for row in rows
            if payload["startTime"] <= row["time"] <= payload["endTime"]
        ][:page_size]

    client.info = info
    return client, calls


def test_fills_pagination_keeps_split_millisecond_and_other_markets():
    rows = [
        {"time": i, "tid": i, "coin": "io:ANTH"}
        for i in range(1, 501)
    ]
    rows += [
        {"time": 500, "tid": 501, "coin": "io:SNDK"},
        {"time": 501, "tid": 502, "coin": "io:SNDK"},
    ]
    client, calls = history_client(rows)
    result = asyncio.run(client.fills_since("0xABC", 0, 1000))
    assert result == rows
    assert [call["startTime"] for call in calls] == [0, 500]
    assert all(call["endTime"] == 1000 for call in calls)
    assert all(call["aggregateByTime"] is False for call in calls)
    assert all(call["user"] == "0xabc" for call in calls)


def test_fills_full_single_millisecond_fails_closed():
    from entropy_bot.market import IncompleteHistoryError

    rows = [{"time": 100, "tid": i, "coin": "io:SNDK"} for i in range(2001)]
    client, _calls = history_client(rows, page_size=2000)
    with pytest.raises(IncompleteHistoryError, match="同一毫秒"):
        asyncio.run(client.fills_since("0xabc", 0, 1000))


def test_fills_retention_boundary_is_unknown_even_when_last_page_short(monkeypatch):
    import entropy_bot.market as module

    monkeypatch.setattr(module, "FILL_HISTORY_LIMIT", 12)
    monkeypatch.setattr(module, "TIME_RANGE_PAGE_SIZE", 5)
    rows = [{"time": i, "tid": i, "coin": "io:SNDK"} for i in range(1, 13)]
    client, _calls = history_client(rows, page_size=5)
    with pytest.raises(module.IncompleteHistoryError, match="历史上限"):
        asyncio.run(client.fills_since("0xabc", 0, 100))


@pytest.mark.parametrize("value", [{"error": "unavailable"}, [{"coin": "io:SNDK"}]])
def test_fills_malformed_history_is_unknown(value):
    from entropy_bot.market import IncompleteHistoryError

    client, _calls = history_client([])

    async def info(_payload):
        return value

    client.info = info
    with pytest.raises(IncompleteHistoryError, match="UNKNOWN"):
        asyncio.run(client.fills_since("0xabc", 0, 100))


def test_funding_paginates_inclusively_and_keeps_signed_official_cashflow():
    rows = [
        {
            "time": i, "hash": f"funding-{i}",
            "delta": {"coin": "io:SNDK", "type": "funding", "usdc": "-0.25"},
        }
        for i in range(1, 503)
    ]
    client, calls = history_client(rows)
    result = asyncio.run(client.user_funding_since("0xabc", 2, 501))
    assert result == rows[1:501]
    assert [call["startTime"] for call in calls] == [2, 501]
    assert all(call["type"] == "userFunding" for call in calls)
    assert all("aggregateByTime" not in call for call in calls)


@pytest.mark.parametrize("margin_mode", ["strictIsolated", "noCross"])
def test_metadata_respects_replacement_margin_mode(margin_mode):
    client, _calls = history_client([])

    async def info(payload):
        if payload["type"] == "perpDexs":
            return [None, {"name": client.cfg.dex}]
        return {"universe": [{
            "name": f"{client.cfg.dex}:{client.cfg.symbol}",
            "szDecimals": 4, "maxLeverage": 10,
            "marginMode": margin_mode, "growthMode": "enabled",
        }]}

    client.info = info
    assert asyncio.run(client.load_meta()).only_isolated is True


def test_malformed_open_orders_cannot_become_empty_snapshot():
    client, _calls = history_client([])

    async def info(_payload):
        return {"error": "temporary failure"}

    client.info = info
    with pytest.raises(RuntimeError, match="不能视为无开放订单"):
        asyncio.run(client.open_orders("0xabc"))


def test_info_request_weights_follow_official_classes():
    assert info_request_weight({"type": "clearinghouseState"}) == 2
    assert info_request_weight({"type": "l2Book"}) == 2
    assert info_request_weight({"type": "userRole"}) == 60
    assert info_request_weight({"type": "activeAssetData"}) == 20
    assert info_request_weight({"type": "userFillsByTime"}) == 40


def test_default_info_budget_leaves_room_for_bot_ui_and_one_check():
    assert InfoRateLimiter().capacity == 350
    assert InfoRateLimiter().capacity * 3 < 1200


def test_info_429_waits_and_retries_read_only_request():
    cfg = load_config(Path(__file__).parents[1] / "config.market_maker.json")

    class Response:
        def __init__(self, status, payload=None, headers=None):
            self.status = status
            self.payload = payload
            self.headers = headers or {}

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def read(self):
            return b""

        async def json(self):
            return self.payload

        def raise_for_status(self):
            if self.status >= 400:
                raise RuntimeError(f"HTTP {self.status}")

    class Session:
        def __init__(self):
            self.responses = [
                Response(429, headers={"Retry-After": "7"}),
                Response(200, payload={"ok": True}),
            ]
            self.calls = 0

        def post(self, *_args, **_kwargs):
            value = self.responses[self.calls]
            self.calls += 1
            return value

    class Limiter:
        def __init__(self):
            self.weights = []
            self.blocks = []

        async def acquire(self, weight):
            self.weights.append(weight)

        async def block_for(self, delay):
            self.blocks.append(delay)

    session = Session()
    client = MarketClient(cfg.market, session)  # type: ignore[arg-type]
    limiter = Limiter()
    client._info_limiter = limiter  # type: ignore[assignment]

    value = asyncio.run(client.info({"type": "activeAssetData"}))

    assert value == {"ok": True}
    assert session.calls == 2
    assert limiter.weights == [20, 20]
    assert limiter.blocks == [7.0]


def test_info_retries_transport_blips_then_fails_closed():
    """Up to two connector-level failures (DNS flap, dual-stack fallback, TLS
    handshake rejected by a bad CDN edge) are absorbed with short retries on
    the read-only /info endpoint; a third consecutive failure propagates to
    the engine's connectivity protection."""
    import aiohttp

    cfg = load_config(Path(__file__).parents[1] / "config.market_maker.json")

    class OkResponse:
        status = 200
        headers: dict = {}

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        def raise_for_status(self):
            return None

        async def json(self):
            return {"ok": True}

    class FlakySession:
        def __init__(self, failures):
            self.failures = failures
            self.calls = 0

        def post(self, *_args, **_kwargs):
            self.calls += 1
            if self.calls <= self.failures:
                raise aiohttp.ClientConnectionError(
                    "Cannot connect to host api.hyperliquid.xyz:443"
                )
            return OkResponse()

    session = FlakySession(failures=2)
    client = MarketClient(cfg.market, session)  # type: ignore[arg-type]
    value = asyncio.run(client.info({"type": "l2Book"}))
    assert value == {"ok": True}
    assert session.calls == 3

    session = FlakySession(failures=3)
    client = MarketClient(cfg.market, session)  # type: ignore[arg-type]
    try:
        asyncio.run(client.info({"type": "l2Book"}))
        raise AssertionError("third consecutive failure must propagate")
    except aiohttp.ClientConnectionError:
        pass
    assert session.calls == 3


def test_history_keeps_official_order_within_one_millisecond():
    rows = [{"time": 100, "tid": 99, "coin": "io:SNDK"},
            {"time": 100, "tid": 1, "coin": "io:SNDK"}]
    client, _calls = history_client(rows)
    assert asyncio.run(client.fills_since("0xabc", 0, 1000)) == rows
