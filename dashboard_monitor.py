"""Official read-only account monitoring with bounded independent caches."""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Callable

import aiohttp

from entropy_bot.config import Config
from entropy_bot.market import MarketClient
from dashboard_snapshot import (
    _number, calculate_exposure, official_account_view, official_order_rows, official_fill_rows,
)


@dataclass(frozen=True)
class MonitorServices:
    LIVE_TEMPLATE: Path
    load_config: Callable
    resolve_live_credentials: Callable
    MarketClient: Callable
    build_fee_model: Callable
    classify_recent_fill_ownership: Callable
    DASHBOARD_ACCOUNT_CACHE_SEC: float
    DASHBOARD_FILLS_CACHE_SEC: float


class LiveMonitor:
    class WalletNotConfigured(RuntimeError):
        """Expected first-run state; never contains credential details."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        symbol: str = "SNDK",
        wallet_id: str = "primary",
        *, services: Callable[[], MonitorServices],
    ) -> None:
        self._services = services
        self.session = session
        self.symbol = symbol.upper()
        self.wallet_id = wallet_id
        self.cfg: Config | None = None
        self.market: MarketClient | None = None
        self.address: str | None = None
        self.credentials_source = "none"
        self.address_derived = False
        self._cache: dict | None = None
        self._cache_at = 0.0
        self._identity_cache: dict | None = None
        self._identity_at = 0.0
        self._fills_cache: list = []
        self._fills_at = 0.0
        self._fills_updated_at_ms = 0
        self._fills_initialized = False
        self._fill_ownership_cache: dict[int, str] = {}
        self._lock = asyncio.Lock()

    async def _initialize(self) -> None:
        deps = self._services()
        if self.cfg is not None:
            return
        base = deps.load_config(deps.LIVE_TEMPLATE)
        cfg = replace(
            base,
            market=replace(base.market, symbol=self.symbol),
            live=replace(base.live, wallet_id=self.wallet_id),
        )
        credentials = deps.resolve_live_credentials(
            cfg, require_private_key=False
        )
        if not credentials.account_address:
            raise self.WalletNotConfigured()
        # SNDK and ANTH monitors are independent clients in this process.  A
        # measured cold monitor used 246 weight for metadata, identity and its
        # first snapshot. 250 avoids waiting a full local rate-limit window;
        # the UI uses local telemetry instead of this monitor while bots run.
        market = deps.MarketClient(cfg.market, self.session, info_capacity=250)
        await market.load_meta()
        # Commit only after all initialization steps succeed. Failed first-run
        # setup or metadata requests must leave the next refresh free to retry.
        self.cfg = cfg
        self.market = market
        self.address = credentials.account_address
        self.credentials_source = credentials.source
        self.address_derived = credentials.account_was_derived

    async def _identity(self) -> dict:
        deps = self._services()
        assert self.market and self.address and self.cfg
        if self._identity_cache and time.monotonic() - self._identity_at < 600:
            return self._identity_cache
        role, abstraction, fees = await asyncio.gather(
            self.market.user_role(self.address),
            self.market.user_abstraction(self.address),
            self.market.user_fees(self.address),
        )
        fee_model = deps.build_fee_model(
            self.market.meta,
            fees,
            self_rebate_pct=self.cfg.fees.entropy_self_rebate_pct,
            referred_benefit_pct=self.cfg.fees.entropy_referred_user_benefit_pct,
        )
        self._identity_cache = {
            "role": role.get("role"),
            "account_abstraction": abstraction,
            "fee_model": fee_model.to_dict(),
        }
        self._identity_at = time.monotonic()
        return self._identity_cache

    async def snapshot(self, *, force: bool = False) -> dict:
        deps = self._services()
        if not force and self._cache is not None:
            if (
                time.monotonic() - self._cache_at < deps.DASHBOARD_ACCOUNT_CACHE_SEC
                or self._lock.locked()
            ):
                return self._cache
        async with self._lock:
            if (
                not force
                and self._cache is not None
                and time.monotonic() - self._cache_at
                < deps.DASHBOARD_ACCOUNT_CACHE_SEC
            ):
                return self._cache
            try:
                await self._initialize()
                assert self.market and self.address and self.cfg
                identity = await self._identity()
                refresh_fills = (
                    not self._fills_initialized
                    or time.monotonic() - self._fills_at
                    >= deps.DASHBOARD_FILLS_CACHE_SEC
                )
                fill_request = (
                    self.market.fills_since(
                        self.address, int((time.time() - 24 * 3600) * 1000)
                    )
                    if refresh_fills
                    else asyncio.sleep(0, result=self._fills_cache)
                )
                (
                    market_snapshot,
                    account_state,
                    spot_state,
                    active_data,
                    open_raw,
                    fill_raw,
                ) = await asyncio.gather(
                    self.market.refresh_context(),
                    self.market.account_state(self.address),
                    self.market.spot_state(self.address),
                    self.market.active_asset_data(self.address),
                    self.market.open_orders(self.address),
                    fill_request,
                )
                if refresh_fills:
                    self._fills_cache = list(fill_raw)
                    self._fills_at = time.monotonic()
                    self._fills_updated_at_ms = int(time.time() * 1000)
                    self._fills_initialized = True
                await self.market.rest_book_once()
                coin = self.market.meta.coin
                ownership_by_oid = await deps.classify_recent_fill_ownership(
                    self.market,
                    self.address,
                    list(fill_raw),
                    coin=coin,
                    cache=self._fill_ownership_cache,
                    now_ms=int(time.time() * 1000),
                )
                mark_px = _number(market_snapshot.mark_px)
                account = official_account_view(
                    account_state, spot_state, active_data, coin=coin, identity=identity
                )
                orders = official_order_rows(open_raw, coin)
                fills = official_fill_rows(fill_raw, coin, ownership_by_oid)
                self._cache = {
                    "ok": True,
                    "updated_at_ms": int(time.time() * 1000),
                    "coin": coin, "role": identity.get("role"), **account,
                    "market": {
                        "best_bid": market_snapshot.book.best_bid,
                        "best_ask": market_snapshot.book.best_ask,
                        "spread_bps": market_snapshot.book.spread_bps,
                        "mark_px": market_snapshot.mark_px,
                        "oracle_px": market_snapshot.oracle_px,
                        "funding": market_snapshot.funding,
                        "day_notional_volume": market_snapshot.day_notional_volume,
                        "growth_mode": self.market.meta.growth_mode,
                        "isolated_only": self.market.meta.only_isolated,
                    },
                    "exposure": calculate_exposure(account["position"]["qty"], mark_px, orders),
                    "open_orders": orders, "recent_fills": fills[:200],
                    # Fills keep their own freshness boundary, independent of
                    # the cheaper account and open-order snapshots.
                    "fills_updated_at_ms": self._fills_updated_at_ms,
                    "fee_model": identity.get("fee_model"),
                }
                self._cache_at = time.monotonic()
                return self._cache
            except self.WalletNotConfigured:
                return {
                    "ok": False,
                    "updated_at_ms": int(time.time() * 1000),
                    "code": "wallet_not_configured",
                    "error": "尚未配置钱包，请在“钱包与配置”填写资金账户和已授权的 API 钱包私钥。",
                }
            except Exception as exc:
                return {
                    "ok": False,
                    "updated_at_ms": int(time.time() * 1000),
                    "error": f"{type(exc).__name__}: 只读账户核验失败",
                }
