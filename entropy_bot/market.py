from __future__ import annotations

import asyncio
from collections import deque
import json
import logging
import socket
import time
from typing import Callable, Optional

import aiohttp

try:
    from websockets.asyncio.client import connect as ws_connect
except ImportError:  # websockets < 14
    from websockets import connect as ws_connect  # type: ignore

from .config import MarketConfig
from .models import MarketMeta, MarketSnapshot, OrderBook

log = logging.getLogger("entropy.market")


_LOW_WEIGHT_INFO_TYPES = {
    "l2Book",
    "allMids",
    "clearinghouseState",
    "orderStatus",
    "spotClearinghouseState",
    "exchangeStatus",
}

# User-scoped websocket channels forwarded to the broker.  Everything here is
# push-based official state: it replaces REST polling weight, while the REST
# endpoints remain the low-frequency authority for audits and recovery.
_USER_WS_CHANNELS = {
    "orderUpdates",
    "userFills",
    "clearinghouseState",
    "openOrders",
    "activeAssetData",
    "spotState",
}

# metaAndAssetCtxs also revalidates isDelisted/growthMode flags, which the
# websocket activeAssetCtx stream does not carry.  While the stream is fresh,
# a slower REST cadence keeps that safety check alive at 1/10th the weight.
META_REVALIDATE_SEC = 300.0

# Time-range endpoints may stop at 500 elements/distinct blocks. Fills also
# have a 2000-row page cap and a rolling 10000-row history window. Paginate
# conservatively at the smaller boundary; never claim a clipped window is
# complete just because the last request happened to succeed.
TIME_RANGE_PAGE_SIZE = 500
FILL_HISTORY_LIMIT = 10_000
MAX_HISTORY_PAGES = 64


class IncompleteHistoryError(RuntimeError):
    """Official history cannot prove a complete, gap-free requested window."""


def build_rest_session() -> aiohttp.ClientSession:
    """Create an HTTP session for Hyperliquid REST and WebSocket traffic.

    Short DNS caching reduces resolver pressure while allowing endpoint
    changes. An explicit IPv4 socket family avoids intermittent failures
    when a local dual-stack route is unavailable.
    """
    return aiohttp.ClientSession(
        trust_env=False,
        connector=aiohttp.TCPConnector(
            family=socket.AF_INET,
            # Short DNS caching reduces resolver pressure while allowing fresh
            # results after a CDN endpoint becomes unavailable.
            ttl_dns_cache=30,
        ),
    )


def info_request_weight(payload: dict) -> int:
    """Conservative Hyperliquid REST weight estimate for local pacing."""
    request_type = str(payload.get("type") or "")
    if request_type == "userRole":
        return 60
    if request_type in _LOW_WEIGHT_INFO_TYPES:
        return 2
    if request_type in {"userFills", "userFillsByTime"}:
        # These endpoints add response-size weight. Normal bot responses are
        # small, but reserve one extra base unit instead of assuming weight 20.
        return 40
    return 20


class InfoRateLimiter:
    """Per-process weighted window with headroom for the UI/other clients.

    Capacity is deliberately supplied per client.  The dual-market live setup
    budgets 450 as each trader's cold-start/burst ceiling and keeps 250 as a
    dashboard stopped-state ceiling. While bots are running, the dashboard
    uses their local reconciled telemetry instead of official REST. Cheap
    open-order-first polling and on-demand fill retrieval leave headroom for
    exchange actions and delayed-fill recovery under the aggregate IP allowance.
    """

    def __init__(self, capacity: int = 350, window_sec: float = 60.0) -> None:
        self.capacity = capacity
        self.window_sec = window_sec
        self._events: deque[tuple[float, int]] = deque()
        self._used = 0
        self._blocked_until = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self, weight: int) -> None:
        while True:
            wait_sec = 0.0
            async with self._lock:
                now = time.monotonic()
                while self._events and now - self._events[0][0] >= self.window_sec:
                    _, expired = self._events.popleft()
                    self._used -= expired
                if now < self._blocked_until:
                    wait_sec = self._blocked_until - now
                elif self._used + weight <= self.capacity:
                    self._events.append((now, weight))
                    self._used += weight
                    return
                elif self._events:
                    wait_sec = max(
                        self._events[0][0] + self.window_sec - now,
                        0.05,
                    )
                else:
                    # A future endpoint weight larger than the configured
                    # budget must still make progress one request at a time.
                    self._events.append((now, weight))
                    self._used += weight
                    return
            await asyncio.sleep(wait_sec)

    async def block_for(self, delay_sec: float) -> None:
        async with self._lock:
            self._blocked_until = max(
                self._blocked_until,
                time.monotonic() + max(delay_sec, 0.0),
            )


class MarketClient:
    def __init__(
        self,
        cfg: MarketConfig,
        session: aiohttp.ClientSession,
        *,
        info_capacity: int = 350,
    ) -> None:
        self.cfg = cfg
        self.session = session
        self.book = OrderBook()
        self.meta: Optional[MarketMeta] = None
        self.snapshot: Optional[MarketSnapshot] = None
        self._info_limiter = InfoRateLimiter(capacity=info_capacity)
        self._info_lock = asyncio.Lock()
        # Websocket session accounting.  ws_session increments on every
        # (re)connect so caches derived from push data can invalidate
        # themselves; l2Book pushes at least every block, so book.alive_ts
        # doubles as a connection heartbeat.
        self.ws_session = 0
        self._ws_ctx_ts = 0.0
        self._last_meta_validation = 0.0

    def ws_alive_within(self, max_age_sec: float) -> bool:
        return (
            self.ws_session > 0
            and self.book.alive_ts > 0
            and time.time() - self.book.alive_ts <= max_age_sec
        )

    @property
    def ws_ctx_age_sec(self) -> float | None:
        if self._ws_ctx_ts <= 0:
            return None
        return time.monotonic() - self._ws_ctx_ts

    async def info(self, payload: dict) -> object:
        timeout = aiohttp.ClientTimeout(total=10)
        request_type = str(payload.get("type") or "unknown")
        weight = info_request_weight(payload)
        async with self._info_lock:
            connect_retries = 0
            attempt = 0
            while attempt < 4:
                await self._info_limiter.acquire(weight)
                try:
                    async with self.session.post(
                        self.cfg.api_url + "/info", json=payload, timeout=timeout
                    ) as response:
                        if response.status != 429:
                            response.raise_for_status()
                            return await response.json()
                        retry_header = response.headers.get("Retry-After", "")
                        try:
                            retry_after = float(retry_header)
                        except ValueError:
                            retry_after = 0.0
                        delay = max(retry_after, min(5.0 * (2**attempt), 30.0))
                        await response.read()
                        await self._info_limiter.block_for(delay)
                        if attempt >= 3:
                            response.raise_for_status()
                except (aiohttp.ClientConnectionError, TimeoutError) as exc:
                    # /info is read-only, so absorbing transport-level blips
                    # (DNS flap, dual-stack fallback, TLS handshake rejected
                    # by a bad CDN edge) with short retries is safe and keeps
                    # routine flicker out of the strategy error budget.
                    # Persistent failure is a real outage: let the engine's
                    # connectivity protection own it.
                    if connect_retries >= 2:
                        raise
                    connect_retries += 1
                    delay = 0.3 * (2 ** (connect_retries - 1))
                    log.warning(
                        "官方 /info 连接抖动 (type=%s): %s；%.1f 秒后重试 (%d/2)",
                        request_type,
                        exc,
                        delay,
                        connect_retries,
                    )
                    await asyncio.sleep(delay)
                    continue
                attempt += 1
                log.warning(
                    "官方 /info 限流: type=%s；%.1fs 后重试 (%d/3)",
                    request_type,
                    delay,
                    attempt,
                )
            raise RuntimeError("不可达的 /info 重试状态")

    async def load_meta(self) -> MarketMeta:
        dexs, meta = await asyncio.gather(
            self.info({"type": "perpDexs"}),
            self.info({"type": "meta", "dex": self.cfg.dex}),
        )
        dex_names = [(x or {}).get("name", "") for x in dexs]  # type: ignore[union-attr]
        if self.cfg.dex not in dex_names:
            raise RuntimeError(f"HIP-3 dex {self.cfg.dex!r} 不存在")
        dex_index = dex_names.index(self.cfg.dex)
        want = f"{self.cfg.dex}:{self.cfg.symbol}"
        universe = meta.get("universe") or []  # type: ignore[union-attr]
        for index, asset in enumerate(universe):
            if asset.get("name") not in (want, self.cfg.symbol):
                continue
            is_delisted = bool(asset.get("isDelisted"))
            growth = asset.get("growthMode") == "enabled"
            result = MarketMeta(
                coin=asset["name"],
                dex=self.cfg.dex,
                symbol=self.cfg.symbol,
                sz_decimals=int(asset["szDecimals"]),
                max_leverage=int(asset.get("maxLeverage") or 1),
                only_isolated=(
                    bool(asset.get("onlyIsolated"))
                    or asset.get("marginMode") in {"strictIsolated", "noCross"}
                ),
                growth_mode=growth,
                deployer_fee_scale=float(asset.get("deployerFeeScale") or 0.0),
                is_delisted=is_delisted,
                dex_index=dex_index,
                universe_index=index,
                asset_id=110000 + (dex_index - 1) * 10000 + index,
            )
            if result.is_delisted:
                raise RuntimeError(f"{result.coin} 已下架，拒绝启动")
            if self.cfg.require_growth_mode and not result.growth_mode:
                raise RuntimeError(f"{result.coin} 当前未开启 Growth Mode，拒绝启动")
            self.meta = result
            self.snapshot = MarketSnapshot(meta=result, book=self.book)
            return result
        raise RuntimeError(f"Entropy 活跃市场里找不到 {want}")

    def _apply_ws_ctx(self, ctx: dict) -> None:
        """Apply a streamed activeAssetCtx update to the market snapshot."""
        if self.snapshot is None or not isinstance(ctx, dict):
            return

        def number(name: str) -> Optional[float]:
            value = ctx.get(name)
            try:
                return float(value) if value not in (None, "") else None
            except (TypeError, ValueError):
                return None

        mark = number("markPx")
        oracle = number("oraclePx")
        if mark is not None:
            self.snapshot.mark_px = mark
        if oracle is not None:
            self.snapshot.oracle_px = oracle
        funding = number("funding")
        if funding is not None:
            self.snapshot.funding = funding
        open_interest = number("openInterest")
        if open_interest is not None:
            self.snapshot.open_interest = open_interest
        day_volume = number("dayNtlVlm")
        if day_volume is not None:
            self.snapshot.day_notional_volume = day_volume
        self.snapshot.context_ts = time.time()
        self._ws_ctx_ts = time.monotonic()

    async def refresh_context(self, *, force_rest: bool = False) -> MarketSnapshot:
        if self.meta is None or self.snapshot is None:
            raise RuntimeError("market metadata not loaded")
        # While the websocket ctx stream is fresh, mark/oracle/funding are
        # already newer than any REST snapshot.  Only the periodic
        # delisting/growth-mode revalidation still needs metaAndAssetCtxs.
        if (
            not force_rest
            and self._ws_ctx_ts > 0
            and time.monotonic() - self._ws_ctx_ts <= self.cfg.context_refresh_sec
            and time.monotonic() - self._last_meta_validation < META_REVALIDATE_SEC
        ):
            return self.snapshot
        response = await self.info({"type": "metaAndAssetCtxs", "dex": self.cfg.dex})
        meta, contexts = response  # type: ignore[misc]
        assets = meta.get("universe") or []
        found = None
        for i, asset in enumerate(assets):
            if asset.get("name") == self.meta.coin:
                if asset.get("isDelisted"):
                    raise RuntimeError(f"{self.meta.coin} 在运行中被下架")
                growth = asset.get("growthMode") == "enabled"
                if self.cfg.require_growth_mode and not growth:
                    raise RuntimeError(f"{self.meta.coin} Growth Mode 已关闭")
                found = contexts[i]
                break
        if found is None:
            raise RuntimeError(f"实时元数据中缺少 {self.meta.coin}")

        def number(name: str) -> Optional[float]:
            value = found.get(name)
            return float(value) if value not in (None, "") else None

        self.snapshot.mark_px = number("markPx")
        self.snapshot.oracle_px = number("oraclePx")
        self.snapshot.funding = number("funding")
        self.snapshot.open_interest = number("openInterest")
        self.snapshot.day_notional_volume = number("dayNtlVlm")
        self.snapshot.context_ts = time.time()
        self._last_meta_validation = time.monotonic()
        return self.snapshot

    async def user_fees(self, address: str) -> dict:
        value = await self.info({"type": "userFees", "user": address.lower()})
        if not isinstance(value, dict):
            raise RuntimeError("userFees 返回格式异常")
        return value

    async def user_role(self, address: str) -> dict:
        value = await self.info({"type": "userRole", "user": address.lower()})
        if not isinstance(value, dict):
            raise RuntimeError("userRole 返回格式异常")
        return value

    async def user_dex_abstraction(self, address: str) -> object:
        return await self.info(
            {"type": "userDexAbstraction", "user": address.lower()}
        )

    async def user_abstraction(self, address: str) -> object:
        return await self.info(
            {"type": "userAbstraction", "user": address.lower()}
        )

    async def account_state(self, address: str) -> dict:
        value = await self.info(
            {"type": "clearinghouseState", "user": address.lower(), "dex": self.cfg.dex}
        )
        if not isinstance(value, dict):
            raise RuntimeError("clearinghouseState 返回格式异常")
        return value

    async def portfolio(self, address: str) -> object:
        return await self.info({"type": "portfolio", "user": address.lower()})

    async def spot_state(self, address: str) -> dict:
        value = await self.info({"type": "spotClearinghouseState", "user": address.lower()})
        return value if isinstance(value, dict) else {}

    async def active_asset_data(self, address: str) -> dict:
        if self.meta is None:
            raise RuntimeError("market metadata not loaded")
        value = await self.info(
            {
                "type": "activeAssetData",
                "user": address.lower(),
                "coin": self.meta.coin,
            }
        )
        if not isinstance(value, dict):
            raise RuntimeError("activeAssetData 返回格式异常")
        return value

    async def open_orders(self, address: str) -> list:
        value = await self.info(
            {
                "type": "frontendOpenOrders",
                "user": address.lower(),
                "dex": self.cfg.dex,
            }
        )
        if not isinstance(value, list):
            raise RuntimeError("frontendOpenOrders 返回格式异常，不能视为无开放订单")
        return value

    async def order_status(self, address: str, oid: str | int) -> dict | None:
        value = await self.info(
            {"type": "orderStatus", "user": address.lower(), "oid": oid}
        )
        return value if isinstance(value, dict) else None

    async def _time_range_history(
        self,
        request_type: str,
        address: str,
        start_ms: int,
        end_ms: int | None = None,
        *,
        history_limit: int | None = None,
    ) -> list[dict]:
        """Read a bounded window with inclusive overlap and stable dedup.

        A full page at one millisecond cannot be paginated safely: adding 1
        would lose any remaining rows at that millisecond. Raise UNKNOWN in
        that case, on a retention cap, or on malformed/inconsistent pages.
        The caller receives no partial result and must not advance its cursor.
        """
        cursor = max(int(start_ms), 0)
        end = int(time.time() * 1000) if end_ms is None else int(end_ms)
        if end < cursor:
            return []
        rows: dict[str, dict] = {}
        for _page_number in range(MAX_HISTORY_PAGES):
            payload = {
                "type": request_type,
                "user": address.lower(),
                "startTime": cursor,
                "endTime": end,
            }
            if request_type == "userFillsByTime":
                payload["aggregateByTime"] = False
            page = await self.info(payload)
            if not isinstance(page, list):
                raise IncompleteHistoryError(f"{request_type}: UNKNOWN，历史返回格式异常")
            max_time = cursor
            for row in page:
                if not isinstance(row, dict):
                    raise IncompleteHistoryError(f"{request_type}: UNKNOWN，历史行格式异常")
                timestamp = row.get("time")
                if isinstance(timestamp, bool) or not isinstance(timestamp, int):
                    raise IncompleteHistoryError(f"{request_type}: UNKNOWN，历史时间缺失/异常")
                if not cursor <= timestamp <= end:
                    raise IncompleteHistoryError(f"{request_type}: UNKNOWN，历史行超出请求窗口")
                max_time = max(max_time, timestamp)
                if request_type == "userFillsByTime":
                    trade_id = row.get("tid")
                    if trade_id is None or not row.get("coin"):
                        raise IncompleteHistoryError(f"{request_type}: UNKNOWN，成交身份缺失")
                    identity = f"{row['coin']}:{trade_id}"
                else:
                    delta = row.get("delta")
                    if (
                        not row.get("hash")
                        or not isinstance(delta, dict)
                        or not delta.get("coin")
                        or delta.get("type") != "funding"
                    ):
                        raise IncompleteHistoryError(f"{request_type}: UNKNOWN，资金费身份缺失")
                    identity = f"{timestamp}:{row['hash']}:{delta['coin']}"
                previous = rows.get(identity)
                if previous is not None and previous != row:
                    raise IncompleteHistoryError(f"{request_type}: UNKNOWN，同一事件内容矛盾")
                rows[identity] = row
            if history_limit is not None and len(rows) >= history_limit:
                raise IncompleteHistoryError(
                    f"{request_type}: UNKNOWN，达到官方最近 {history_limit} 条历史上限"
                )
            if len(page) < TIME_RANGE_PAGE_SIZE:
                return sorted(rows.values(), key=lambda row: row["time"])
            if max_time <= cursor:
                raise IncompleteHistoryError(
                    f"{request_type}: UNKNOWN，同一毫秒历史满页，无法证明完整性"
                )
            # Both bounds are inclusive. Repeat the final millisecond so a
            # split timestamp is retained and deduplicated instead of skipped.
            cursor = max_time
        raise IncompleteHistoryError(f"{request_type}: UNKNOWN，历史分页未能完整收敛")

    async def fills_since(
        self, address: str, start_ms: int, end_ms: int | None = None
    ) -> list[dict]:
        return await self._time_range_history(
            "userFillsByTime", address, start_ms, end_ms,
            history_limit=FILL_HISTORY_LIMIT,
        )

    async def user_funding_since(
        self, address: str, start_ms: int, end_ms: int | None = None
    ) -> list[dict]:
        """Official signed funding cashflows only within the requested window.

        This does not infer funding before a ledger's start time. Preserve
        coin/hash/time/delta.usdc so callers can scope and deduplicate events.
        """
        return await self._time_range_history("userFunding", address, start_ms, end_ms)

    async def recent_fills(self, address: str) -> list:
        """Read the alternate recent-fill view for delayed-detail recovery."""
        value = await self.info(
            {
                "type": "userFills",
                "user": address.lower(),
                "aggregateByTime": False,
            }
        )
        if not isinstance(value, list):
            raise IncompleteHistoryError("userFills: UNKNOWN，最近成交返回格式异常")
        return value

    async def user_rate_limit(self, address: str) -> dict:
        """Authoritative address-based action budget (cap grows with volume)."""
        value = await self.info(
            {"type": "userRateLimit", "user": address.lower()}
        )
        if not isinstance(value, dict):
            raise RuntimeError("userRateLimit 返回格式异常")
        return value

    async def rest_book_once(self) -> OrderBook:
        if self.meta is None:
            raise RuntimeError("market metadata not loaded")
        value = await self.info({"type": "l2Book", "coin": self.meta.coin, "nSigFigs": 5})
        if not isinstance(value, dict):
            raise RuntimeError("l2Book 返回格式异常")
        self.book.apply_hl(value.get("levels") or [])
        return self.book

    def _subscriptions(self, user_address: str | None) -> list[dict]:
        assert self.meta is not None
        subs: list[dict] = [
            {"type": "l2Book", "coin": self.meta.coin, "fast": True},
            {"type": "activeAssetCtx", "coin": self.meta.coin},
        ]
        if user_address:
            user = user_address.lower()
            subs.extend(
                [
                    {"type": "orderUpdates", "user": user},
                    {
                        "type": "userFills",
                        "user": user,
                        "aggregateByTime": False,
                    },
                    {
                        "type": "clearinghouseState",
                        "user": user,
                        "dex": self.cfg.dex,
                    },
                    {"type": "openOrders", "user": user, "dex": self.cfg.dex},
                    {
                        "type": "activeAssetData",
                        "user": user,
                        "coin": self.meta.coin,
                    },
                    {"type": "spotState", "user": user},
                ]
            )
        return subs

    async def run_book(self, stop: asyncio.Event, notify: Callable[[], None]) -> None:
        await self.run_feeds(stop, notify)

    async def run_feeds(
        self,
        stop: asyncio.Event,
        notify: Callable[[], None],
        *,
        user_address: str | None = None,
        on_user_event: Callable[[str, object], None] | None = None,
    ) -> None:
        """One multiplexed websocket: book + asset ctx + push-based user state.

        Received messages are free of REST weight, so all steady-state
        official polling can ride this connection.  REST stays authoritative:
        the broker falls back to it whenever a channel is stale, and audits
        keep running on their own low-frequency clock.
        """
        if self.meta is None:
            raise RuntimeError("market metadata not loaded")
        backoff = 1.0
        while not stop.is_set():
            ping_task = None
            try:
                async with ws_connect(
                    self.cfg.ws_url,
                    max_size=2**23,
                    open_timeout=10,
                    ping_interval=15,
                    ping_timeout=15,
                ) as ws:
                    self.book.clear()
                    self.ws_session += 1
                    self._ws_ctx_ts = 0.0
                    if on_user_event is not None:
                        # New session: push caches from an old connection must
                        # not satisfy freshness checks any longer.
                        on_user_event("ws_reset", {"session": self.ws_session})
                    for subscription in self._subscriptions(user_address):
                        await ws.send(
                            json.dumps(
                                {
                                    "method": "subscribe",
                                    "subscription": subscription,
                                }
                            )
                        )

                    async def pinger() -> None:
                        while True:
                            await asyncio.sleep(5)
                            await ws.send(json.dumps({"method": "ping"}))

                    ping_task = asyncio.create_task(pinger(), name="hl-ws-pinger")
                    log.info(
                        "官方行情/账户推送已连接: %s (用户频道=%s)",
                        self.meta.coin,
                        "on" if user_address else "off",
                    )
                    async for raw in ws:
                        message = json.loads(raw)
                        self.book.touch()
                        channel = message.get("channel")
                        if channel == "l2Book":
                            data = message.get("data") or {}
                            if data.get("coin") == self.meta.coin:
                                self.book.apply_hl(data.get("levels") or [])
                                notify()
                        elif channel == "activeAssetCtx":
                            data = message.get("data") or {}
                            if data.get("coin") == self.meta.coin:
                                self._apply_ws_ctx(data.get("ctx") or {})
                        elif (
                            channel in _USER_WS_CHANNELS
                            and on_user_event is not None
                        ):
                            on_user_event(channel, message.get("data"))
                            if channel in ("orderUpdates", "userFills"):
                                # Wake the engine loop so fills re-arm quotes
                                # immediately instead of waiting a poll tick.
                                notify()
                        if stop.is_set():
                            break
                    backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("行情连接异常: %s；%.0fs 后重连", exc, backoff)
            finally:
                if ping_task is not None:
                    ping_task.cancel()
            self.book.ready = False
            notify()
            if not stop.is_set():
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)
