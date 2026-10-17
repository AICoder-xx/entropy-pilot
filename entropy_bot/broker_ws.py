"""Push-feed ingestion, freshness and official-view caches."""
from __future__ import annotations

from .broker_protocol import CANCELED_ORDER_STATES
from .broker_protocol import WS_STALE_FALLBACK_SEC
from .broker_protocol import _order_state
import time
import logging

log = logging.getLogger("entropy.broker")

class LiveWsMixin:
    """Push-feed ingestion, freshness and official-view caches. Shared state is initialized only by LiveBroker."""

    def ingest_ws_event(self, channel: str, data: object) -> None:
        """Apply one push message from the shared market websocket.

        Runs inside the market feed task: it must never raise, and it only
        updates caches/local estimates.  Every decision that places or
        cancels orders still goes through the sync/reconcile paths, which
        fall back to REST whenever these caches are stale.
        """
        try:
            self._ingest_ws_event(channel, data)
        except Exception:
            self._ws_open_orders = None
            self._last_rest_sweep = float("-inf")
            self._last_account_sync = float("-inf")
            log.exception("忽略无法解析的 WS 推送: channel=%s", channel)

    def _ingest_ws_event(self, channel: str, data: object) -> None:
        if channel == "ws_reset":
            if isinstance(data, dict) and data.get("session") is not None:
                self._ws_session = int(data["session"])
            self._ws_open_orders = None
            self._ws_clearinghouse = None
            self._ws_spot = None
            self._ws_active_asset = None
            return
        if not self._ws_enabled:
            return
        if channel == "orderUpdates" and isinstance(data, list):
            now = time.monotonic()
            for row in data:
                if not isinstance(row, dict):
                    continue
                order = row.get("order") or {}
                if order.get("coin") != self.meta.coin:
                    continue
                cloid = str(order.get("cloid") or "")
                if not cloid:
                    continue
                pending_ioc = self._pending_ioc_by_identity(
                    cloid=cloid, oid=order.get("oid")
                )
                if pending_ioc is not None:
                    self._mark_ioc_proven(pending_ioc)
                    try:
                        pending_oid = int(order.get("oid") or 0)
                    except (TypeError, ValueError):
                        pending_oid = 0
                    if pending_oid:
                        self._register_ioc_identity(pending_ioc, pending_oid)
                self._bind_active_order_identity(
                    cloid=cloid, oid=order.get("oid")
                )
                # Synthesize the REST orderStatus payload shape so downstream
                # reconciliation code paths stay byte-for-byte identical.
                self._ws_order_status[cloid] = {
                    "mono": now,
                    "payload": {
                        "status": "order",
                        "order": {
                            "status": str(row.get("status") or ""),
                            "statusTimestamp": row.get("statusTimestamp"),
                            "order": dict(order),
                        },
                    },
                }
            if len(self._ws_order_status) > 4000:
                oldest = sorted(
                    self._ws_order_status.items(), key=lambda kv: kv[1]["mono"]
                )
                self._ws_order_status = dict(oldest[-2000:])
            return
        if channel == "userFills" and isinstance(data, dict):
            rows = data.get("fills") or []
            if not isinstance(rows, list):
                return
            for raw in sorted(
                (r for r in rows if isinstance(r, dict)),
                key=lambda x: int(x.get("time") or 0),
            ):
                row_time = self._ingest_fill_row(raw)
                self._last_ws_fill_ms = max(self._last_ws_fill_ms, row_time)
            # The REST cursor (_last_fill_ms) is intentionally NOT advanced
            # here: the periodic authoritative sweep re-reads the same window
            # and the tid dedupe set absorbs the overlap.  A silently dropped
            # WS row therefore cannot create a permanent gap.
            return
        if channel == "clearinghouseState" and isinstance(data, dict):
            if str(data.get("dex") or "") != self.cfg.market.dex:
                return
            inner = data.get("clearinghouseState")
            if isinstance(inner, dict):
                self._ws_clearinghouse = inner
                self._apply_ws_account()
            return
        if channel == "openOrders" and isinstance(data, dict):
            if str(data.get("dex") or "") != self.cfg.market.dex:
                return
            rows = data.get("orders")
            if isinstance(rows, list):
                self._ws_open_orders = rows
                for raw in rows:
                    if not isinstance(raw, dict) or raw.get("coin") != self.meta.coin:
                        continue
                    pending_ioc = self._pending_ioc_by_identity(
                        cloid=str(raw.get("cloid") or ""), oid=raw.get("oid")
                    )
                    if pending_ioc is not None:
                        self._mark_ioc_proven(pending_ioc)
                        try:
                            pending_oid = int(raw.get("oid") or 0)
                        except (TypeError, ValueError):
                            pending_oid = 0
                        if pending_oid:
                            self._register_ioc_identity(pending_ioc, pending_oid)
                    self._bind_active_order_identity(
                        cloid=str(raw.get("cloid") or ""), oid=raw.get("oid")
                    )
            return
        if channel == "activeAssetData" and isinstance(data, dict):
            if data.get("coin") == self.meta.coin:
                self._ws_active_asset = data
                self._apply_ws_account()
            return
        if channel == "spotState" and isinstance(data, dict):
            if isinstance(data.get("balances"), list):
                self._ws_spot = data
                self._apply_ws_account()
            return

    def _ws_connection_alive(self) -> bool:
        if not self._ws_enabled:
            return False
        ws_session = getattr(self.market, "ws_session", 0)
        alive_check = getattr(self.market, "ws_alive_within", None)
        return (
            self._ws_session == ws_session
            and callable(alive_check)
            and bool(alive_check(WS_STALE_FALLBACK_SEC))
        )

    def _ws_open_view(self) -> tuple[set[str], set[int]] | None:
        """Same-market open-order identity sets from the push snapshot."""
        if self._ws_open_orders is None or not self._ws_connection_alive():
            return None
        cloids: set[str] = set()
        oids: set[int] = set()
        for raw in self._ws_open_orders:
            if not isinstance(raw, dict) or raw.get("coin") != self.meta.coin:
                continue
            cloid = raw.get("cloid")
            if cloid:
                cloids.add(str(cloid))
            try:
                oids.add(int(raw.get("oid")))
            except (TypeError, ValueError):
                pass
        return cloids, oids

    def ws_healthy(self) -> bool:
        return self._ws_open_view() is not None

    def _ws_terminal_payload(self, cloid: str) -> dict | None:
        """Immutable terminal status from push events (safe at any age).

        Cloids are unique per order in this bot, and filled/canceled are
        absorbing states, so a terminal push event can be trusted even after
        the connection died.  Non-terminal events are never returned.
        """
        entry = self._ws_order_status.get(cloid)
        if entry is None:
            return None
        payload = entry["payload"]
        state = _order_state(payload)
        if state == "filled" or state in CANCELED_ORDER_STATES:
            return payload
        return None

    async def _order_status_view(self, cloid: str) -> dict | None:
        payload = self._ws_terminal_payload(cloid)
        if payload is not None:
            return payload
        return await self.market.order_status(self.address, cloid)

    def _apply_ws_account(self) -> None:
        """Refresh account state from push caches when a complete set exists.

        The REST account sync keeps running on its own clock as the
        authority; this only shortens the latency between authoritative
        snapshots (position caps react to fills within one push instead of
        one poll interval).
        """
        inner = self._ws_clearinghouse
        if inner is None:
            return
        try:
            payload_time = int(inner.get("time") or 0)
        except (TypeError, ValueError):
            payload_time = 0
        # A clearinghouse snapshot older than the newest locally applied fill
        # would resurrect a stale position estimate; wait for the next push.
        if payload_time and payload_time < max(self._last_applied_fill_ms, self._position_snapshot_ms):
            return
        try:
            if self.account_abstraction == "unifiedAccount":
                if self._ws_spot is None or self._ws_active_asset is None:
                    return
                self._apply_account_payload(
                    inner, self._ws_spot, self._ws_active_asset
                )
            else:
                self._apply_account_payload(inner, {}, self._ws_active_asset or {})
        except Exception:
            self._ws_open_orders = None
            self._last_rest_sweep = float("-inf")
            self._last_account_sync = float("-inf")
            log.exception("WS 账户推送解析失败；等待 REST 权威刷新")
