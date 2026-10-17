"""Shared broker contract and durable order-event recording."""
from __future__ import annotations

from contextlib import contextmanager
from collections.abc import Iterator
from .storage import order_intent_is_pending

from .broker_protocol import CloidFactory
from .config import Config
from .models import AccountState
from .models import FeeModel
from .models import Fill
from .models import MarketMeta
from .models import OrderBook
from .models import OrderIntent
from .models import TrackedOrder
from typing import Callable
from typing import Iterable
import abc
import time

class Broker(abc.ABC):
    def __init__(self, cfg: Config, meta: MarketMeta, fees: FeeModel) -> None:
        self.cfg = cfg
        self.meta = meta
        self.fees = fees
        self.cloids = CloidFactory()
        self.orders_by_role: dict[str, TrackedOrder] = {}
        self._fills: list[Fill] = []
        self.account = AccountState()
        # Official-state drift is kept as counts only.  The engine can fail
        # closed without persisting or exposing exchange identifiers.
        self.official_untracked_bot_orders = 0
        self.official_foreign_orders = 0
        self.official_leverage_type: str | None = None
        self.official_leverage_value: int | None = None
        self.requotes_modified = 0
        self.requotes_replaced = 0
        # Order lifecycle events for the local replay ledger.  The engine
        # drains these into state/*_orders.jsonl alongside fills/markouts.
        self._order_events: list[dict] = []
        # Grid mode can provide a synchronous, fsync-backed intent journal.
        # A failed journal write must happen BEFORE any non-idempotent send.
        # Ordinary cancellation/reconciliation logging does not use this hook.
        self.order_intent_sink: Callable[[dict], None] | None = None
        self._inflight_order_intents: dict[str, dict] = {}

    @contextmanager
    def _submission_scope(
        self, rows: dict[str, dict], preserve: Callable[[str, dict], None],
    ) -> Iterator[None]:
        """Keep every batch identity before awaiting send or reconciliation.

        The active predecessor mapping must stay intact while a modify is in
        flight so concurrent fill pushes use the correct incarnation. On every
        exit (including cancellation/failed queries), transfer unresolved rows
        to the normal UNKNOWN maps before returning control to the engine.
        Reuse the restart-journal predicate so in-memory and disk gates agree.
        """
        self._inflight_order_intents.update(rows)
        try:
            yield
        finally:
            for cloid, row in rows.items():
                if order_intent_is_pending(row):
                    preserve(cloid, row)
                self._inflight_order_intents.pop(cloid, None)

    def _note_order_event(
        self, event: str, *, durable_intent: bool = False, **fields: object
    ) -> dict:
        row: dict = {"time_ms": int(time.time() * 1000), "event": event}
        row.update(fields)
        if durable_intent and self.order_intent_sink is not None:
            self.order_intent_sink(dict(row))
        self._order_events.append(row)
        # Bounded memory if the engine stalls; the ledger is best-effort.
        if len(self._order_events) > 10_000:
            del self._order_events[:5_000]
        return row

    def drain_order_events(self) -> list[dict]:
        events, self._order_events = self._order_events, []
        return events

    @abc.abstractmethod
    async def start(self) -> None: ...

    @abc.abstractmethod
    async def place(self, intents: list[OrderIntent], book: OrderBook) -> None: ...

    @abc.abstractmethod
    async def cancel_roles(self, roles: Iterable[str]) -> None: ...

    @abc.abstractmethod
    async def sync(self, book: OrderBook) -> None: ...

    @abc.abstractmethod
    async def flatten(self, book: OrderBook, slippage_bps: float) -> None: ...

    async def cancel_all(self) -> None:
        await self.cancel_roles(list(self.orders_by_role))

    async def modify_orders(
        self, pairs: list[tuple[str, OrderIntent]], book: OrderBook
    ) -> None:
        """Reprice tracked roles.  Default: plain cancel + place.

        LiveBroker overrides this with atomic batchModify.  The fallback keeps
        PaperBroker and any degraded path semantically identical: the role is
        released first, then re-placed at the new price.
        """
        roles = [role for role, _ in pairs]
        self.requotes_modified += len(roles)
        await self.cancel_roles(roles)
        replacements = [
            intent for role, intent in pairs if role not in self.orders_by_role
        ]
        if replacements:
            await self.place(replacements, book)

    def ws_healthy(self) -> bool:
        return False

    def drain_fills(self) -> list[Fill]:
        fills, self._fills = self._fills, []
        return fills
