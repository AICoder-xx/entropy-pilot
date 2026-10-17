"""Order identities, exchange-state protocol and reconciliation errors."""
from __future__ import annotations

from .models import AccountState
import math
import os
import time

# Push-based views are only trusted while the shared websocket is visibly
# alive (l2Book pushes at least every block, so silence beyond this window
# means the connection is degraded and REST must take over).
WS_STALE_FALLBACK_SEC = 10.0
# How long _cancel_cloids waits for orderUpdates to confirm terminal states
# before falling back to one authoritative REST open-orders read.
WS_CANCEL_CONFIRM_SEC = 1.5
# A timed-out cancel is safe to resend only after a fresh authoritative
# open-orders snapshot proves that the exact Cloid is still resting.  Keep
# both the cadence and the number of signing attempts bounded; the wider
# reconciliation grace remains the final fail-closed deadline.
CANCEL_RETRY_INTERVAL_SEC = 5.0
CANCEL_MAX_SUBMIT_ATTEMPTS = 3

BOT_CLOID_MAGIC = 0x0E170B07
CANCELED_ORDER_STATES = {
    "canceled",
    "cancelled",
    "rejected",
    "margincanceled",
    "vaultwithdrawalcanceled",
    "openinterestcapcanceled",
    "selftradecanceled",
    "reduceonlycanceled",
    "siblingfilledcanceled",
    "delistedcanceled",
    "liquidatedcanceled",
    "scheduledcancel",
    "tickrejected",
    "mintradentlrejected",
    "perpmarginrejected",
    "reduceonlyrejected",
    "badalopxrejected",
    "ioccancelrejected",
    "badtriggerpxrejected",
    "marketordernoliquidityrejected",
    "positionincreaseatopeninterestcaprejected",
    "positionflipatopeninterestcaprejected",
    "tooaggressiveatopeninterestcaprejected",
    "openinterestincreaserejected",
    "insufficientspotbalancerejected",
    "oraclerejected",
    "perpmaxpositionrejected",
}
# Keep official-view contradictions bounded at one minute, but allow the
# separately paginated userFills feed longer to catch up after orderStatus has
# already reported a fill.  Entropy/HIP-3 has shown repeatable 60-120 second
# detail lag; during this window the role mapping stays reserved, so extending
# it cannot create a duplicate replacement order.
ORDER_RECONCILIATION_GRACE_SEC = 60.0
FILL_RECONCILIATION_GRACE_SEC = 300.0
RETIRED_ORDER_FILL_GRACE_SEC = 30 * 60.0
STARTUP_EXTERNAL_ACTIVITY_QUIET_SEC = 15 * 60


class PendingOrderReconciliation(RuntimeError):
    """Official order views disagree but the strategy can wait safely."""

    code = "ORDER_STATE_UNCERTAIN"


class PendingFillReconciliation(PendingOrderReconciliation):
    """The exchange reports a fill whose userFills row is not visible yet."""


class PendingCancelReconciliation(PendingOrderReconciliation):
    """A bounded cancel attempt is still reported open by official state."""


class UncertainIocOutcome(RuntimeError):
    """An IOC send cannot yet be proven accepted or rejected.

    Callers must not submit another IOC while this exception is unresolved.
    ``LiveBroker`` retains the cloid locally and rechecks it read-only before
    allowing any later IOC action.
    """

    code = "ORDER_STATE_UNCERTAIN"


def _http_status_code(exc: BaseException) -> int:
    """Return an HTTP status carried by SDK/aiohttp exception chains."""

    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        for name in ("status_code", "status"):
            try:
                value = int(getattr(current, name, 0) or 0)
            except (TypeError, ValueError):
                value = 0
            if value:
                return value
        current = current.__cause__ or current.__context__
    return 0


def _order_state(status: dict | None) -> str:
    order_info = (status or {}).get("order") or {}
    value = str(order_info.get("status") or "").lower()
    return value.replace("-", "").replace("_", "")


def _reported_filled_qty(status: dict | None) -> float:
    """Return exchange-reported cumulative fill for an orderStatus payload."""
    if not status or status.get("status") != "order":
        return 0.0
    outer = status.get("order") or {}
    inner = outer.get("order") or {}
    try:
        return max(
            float(inner.get("origSz") or 0.0) - float(inner.get("sz") or 0.0),
            0.0,
        )
    except (TypeError, ValueError):
        return 0.0


def _positive_order_id(raw: object) -> int | None:
    if isinstance(raw, bool):
        return None
    try:
        value = int(raw)
    except (TypeError, ValueError, OverflowError):
        return None
    return value if value > 0 and str(value) == str(raw) else None


def recovered_order_identity(status: dict | None, cloid: str, coin: str) -> int | None:
    """Require positive, matching identity evidence before releasing a restart gate."""
    if not isinstance(status, dict) or status.get("status") != "order":
        return None
    outer = status.get("order")
    inner = outer.get("order") if isinstance(outer, dict) else None
    if not isinstance(inner, dict):
        return None
    if str(inner.get("cloid") or "").lower() != cloid.lower() or inner.get("coin") != coin:
        return None
    return _positive_order_id(inner.get("oid"))


def allocation_state(account: AccountState) -> tuple:
    """Values which invalidate an allocation after awaiting an order mutation."""
    return (
        account.position_qty, account.entry_px, account.volume_usd,
        account.equity_actual, account.available_to_buy_usd, account.available_to_sell_usd,
    )


def _validated_positions(state: dict) -> list[dict]:
    if not isinstance(state, dict) or not isinstance(state.get("assetPositions"), list):
        raise RuntimeError("官方仓位响应缺少 assetPositions；不能解释为空仓")
    positions = state["assetPositions"]
    for item in positions:
        position = item.get("position") if isinstance(item, dict) else None
        if not isinstance(position, dict) or not position.get("coin"):
            raise RuntimeError("官方仓位行缺少市场或 position")
        for field in ("szi", "entryPx", "unrealizedPnl", "liquidationPx"):
            raw = position.get(field)
            if field != "szi" and raw in (None, ""):
                continue
            if raw in (None, "") or isinstance(raw, bool) or not math.isfinite(float(raw)):
                raise RuntimeError(f"官方仓位 {field} 无效；保留上次仓位")
        if float(position["szi"]) != 0 and float(position.get("entryPx") or 0) <= 0:
            raise RuntimeError("官方非零仓位缺少有效持仓成本")
    return positions


def is_bot_cloid(value: object) -> bool:
    try:
        return int(str(value), 16) >> 96 == BOT_CLOID_MAGIC
    except (TypeError, ValueError):
        return False


def order_ownership(status: dict | None) -> tuple[str, str | None]:
    """Classify an authoritative orderStatus payload by Cloid namespace."""
    if not isinstance(status, dict) or status.get("status") != "order":
        return "unknown", None
    outer = status.get("order")
    if not isinstance(outer, dict):
        return "unknown", None
    inner = outer.get("order")
    if not isinstance(inner, dict):
        return "unknown", None
    cloid = inner.get("cloid")
    if is_bot_cloid(cloid):
        return "bot", str(cloid)
    return "external", None


class CloidFactory:
    def __init__(self) -> None:
        self.run = int.from_bytes(os.urandom(4), "big")
        self.seq = 0

    def next(self) -> str:
        self.seq += 1
        raw = (
            (BOT_CLOID_MAGIC << 96)
            | (self.run << 64)
            | (int(time.time() * 1000) & 0xFFFFFFFFFFFF) << 16
            | (self.seq & 0xFFFF)
        )
        return f"{raw:#034x}"


class PaidActionRecoveryUnavailable(RuntimeError):
    """Paid action quota has no crash-safe payment journal yet."""

    code = "PAID_ACTION_RECOVERY_UNAVAILABLE"

    def __init__(self) -> None:
        super().__init__(
            "自动购买动作额度缺少持久支付对账，当前禁止执行；"
            "请关闭 runtime.auto_reserve_actions，并在交易所界面管理动作额度"
        )
