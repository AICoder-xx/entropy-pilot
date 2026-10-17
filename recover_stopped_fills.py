from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

import aiohttp

import audit_live as audit
from entropy_bot.broker import order_ownership
from entropy_bot.config import load_config
from entropy_bot.credentials import resolve_live_credentials
from entropy_bot.market import MarketClient
from entropy_bot.storage import append_jsonl


ROOT = Path(__file__).resolve().parent


class MissingRecoveryAccountError(RuntimeError):
    """Fail closed before any official request when account identity is absent."""


def recovery_account_address(credentials: object) -> str:
    address = getattr(credentials, "account_address", None)
    if not isinstance(address, str) or not address:
        raise MissingRecoveryAccountError(
            "停机对账缺少有效账户地址：请先在新目录配置 "
            "wallet.local.json，再重试只读对账"
        )
    return address

if sys.stdout and sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass


def _safe_slug(value: str) -> str:
    return "".join(
        char.lower() if char.isalnum() else "_" for char in value
    ).strip("_") or "primary"


def paths_for(
    symbol: str, wallet_id: str = "primary"
) -> tuple[Path, Path, Path, Path]:
    slug = symbol.lower()
    profile = f"live_market_maker_io_{symbol}"
    wallet_slug = _safe_slug(wallet_id)
    if wallet_slug == "primary":
        config_path = ROOT / f"config.live.ui.{slug}.runtime.json"
        control_path = ROOT / "state" / f"ui_control_{slug}.json"
        state_dir = ROOT / "state"
    else:
        config_path = ROOT / f"config.live.ui.{wallet_slug}.{slug}.runtime.json"
        control_path = (
            ROOT / "state" / f"ui_control_{wallet_slug}.{slug}.json"
        )
        state_dir = ROOT / "state" / "wallets" / wallet_slug
    return (
        config_path,
        control_path,
        state_dir / f"{profile}_summary.json",
        state_dir / f"{profile}_fills.jsonl",
    )


def normalized_fill(raw: dict) -> dict:
    return {
        "time_ms": int(raw.get("time") or 0),
        "oid": int(raw.get("oid") or 0),
        "is_buy": str(raw.get("side") or "").upper() == "B",
        "price": float(raw.get("px") or 0),
        "qty": float(raw.get("sz") or 0),
        "fee_usd": float(raw.get("fee") or 0),
        "liquidity": "taker" if bool(raw.get("crossed")) else "maker",
        "realized_pnl_delta": float(raw.get("closedPnl") or 0),
    }


IOC_RECOVERY_ROLES = {
    "risk:inventory-ioc",
    "risk:emergency-flatten",
}
IOC_FILL_DETAIL_GRACE_MS = 300_000
NON_IDEMPOTENT_ORDER_EVENTS = {"place", "modify"}


def ioc_identity_roles(orders_path: Path, session_id: str) -> dict[tuple[int, str], str]:
    """Load fail-closed IOC risk roles from the stopped session ledger.

    ``ioc_identity`` rows provide the strongest exact oid+cloid identity.  The
    action row is written before the non-idempotent send, so its cloid-only
    identity is also retained as a crash-window fallback when the process dies
    before it can persist an oid.  Exact identities always win at lookup time;
    ambiguous identities are discarded instead of guessing a risk role.
    """

    candidates: dict[tuple[int, str], set[str]] = defaultdict(set)
    for row in audit.read_jsonl(orders_path):
        if str(row.get("session_id") or "") != session_id:
            continue
        event = str(row.get("event") or "")
        cloid = str(row.get("cloid") or "").lower()
        role = str(row.get("role") or "")
        outcome = str(row.get("outcome") or "").lower()
        if not cloid or role not in IOC_RECOVERY_ROLES:
            continue
        if event == "ioc_identity":
            try:
                oid = int(row.get("oid") or 0)
            except (TypeError, ValueError):
                continue
            if oid <= 0:
                continue
        elif (
            event == "inventory_ioc"
            and role == "risk:inventory-ioc"
            and outcome != "rejected"
        ):
            oid = 0
        elif (
            event == "flatten"
            and role == "risk:emergency-flatten"
            and outcome != "rejected"
        ):
            oid = 0
        else:
            continue
        candidates[(oid, cloid)].add(role)
    return {
        key: next(iter(roles))
        for key, roles in candidates.items()
        if len(roles) == 1
    }


def ioc_role_for_fill(
    roles: dict[tuple[int, str], str], oid: int, cloid: str
) -> str | None:
    """Prefer exact oid+cloid evidence, then the same-session action cloid."""

    normalized = cloid.lower()
    return roles.get((int(oid), normalized)) or roles.get((0, normalized))


def ioc_identity_records(orders_path: Path, session_id: str) -> list[dict]:
    """Return unambiguous IOC identities, preferring oid+cloid over action-only."""

    roles = ioc_identity_roles(orders_path, session_id)
    timestamps: dict[tuple[int, str], int] = {}
    for row in audit.read_jsonl(orders_path):
        if str(row.get("session_id") or "") != session_id:
            continue
        cloid = str(row.get("cloid") or "").lower()
        if not cloid:
            continue
        try:
            oid = int(row.get("oid") or 0)
            time_ms = int(row.get("time_ms") or 0)
        except (TypeError, ValueError):
            continue
        event = str(row.get("event") or "")
        if event in {"inventory_ioc", "flatten"}:
            oid = 0
        elif event != "ioc_identity" or oid <= 0:
            continue
        key = (oid, cloid)
        if key in roles:
            timestamps[key] = max(timestamps.get(key, 0), time_ms)

    exact_cloids = {cloid for oid, cloid in roles if oid > 0}
    return [
        {
            "oid": oid,
            "cloid": cloid,
            "role": role,
            "time_ms": timestamps.get((oid, cloid), 0),
        }
        for (oid, cloid), role in roles.items()
        if oid > 0 or cloid not in exact_cloids
    ]


def uncertain_order_identity_records(
    orders_path: Path, session_id: str
) -> list[dict]:
    """Load ordinary place/modify identities that were not durably resolved.

    ``unknownOid`` is not proof that a signed non-idempotent action never
    landed.  A persisted ``submitted``/``send_uncertain`` row, or any newer
    outcome that still lacks the replacement oid, therefore survives a process
    restart as a cloid-only recovery guard.  Explicit exchange rejections are
    excluded.
    """

    ledger_rows = audit.read_jsonl(orders_path)
    positive_by_cloid: dict[str, dict] = {}
    for row in ledger_rows:
        if str(row.get("session_id") or "") != session_id:
            continue
        row_cloid = str(row.get("cloid") or "").lower()
        try:
            row_oid = int(row.get("oid") or 0)
        except (TypeError, ValueError):
            row_oid = 0
        if row_cloid and row_oid > 0:
            positive_by_cloid[row_cloid] = row
    resolved_cloids: set[str] = set()
    for row in ledger_rows:
        if (
            str(row.get("session_id") or "") != session_id
            or str(row.get("event") or "") != "order_identity"
        ):
            continue
        try:
            resolved_oid = int(row.get("oid") or 0)
        except (TypeError, ValueError):
            resolved_oid = 0
        resolved_cloid = str(row.get("cloid") or "").lower()
        if resolved_oid > 0 and resolved_cloid:
            resolved_cloids.add(resolved_cloid)
    by_cloid: dict[str, dict] = {}
    for row in ledger_rows:
        if str(row.get("session_id") or "") != session_id:
            continue
        event = str(row.get("event") or "")
        if event not in NON_IDEMPOTENT_ORDER_EVENTS:
            continue
        cloid = str(row.get("cloid") or "").lower()
        outcome = str(row.get("outcome") or "").lower()
        if (
            not cloid
            or cloid in resolved_cloids
            or not outcome
            or outcome in {"rejected", "rate_limited"}
        ):
            continue
        try:
            oid = int(row.get("oid") or 0)
            time_ms = int(row.get("time_ms") or 0)
        except (TypeError, ValueError):
            oid = 0
            time_ms = 0
        if outcome not in {"submitted", "send_uncertain"} and oid > 0:
            continue
        # A modify action's initially logged oid belongs to the previous order,
        # never to the generated replacement cloid.  Keep recovery cloid-only.
        record = {
            "oid": 0 if event == "modify" else oid,
            "cloid": cloid,
            "role": str(row.get("role") or ""),
            "event": event,
            "outcome": outcome,
            "time_ms": time_ms,
        }
        if event == "modify":
            previous_cloid = str(row.get("previous_cloid") or "").lower()
            previous = positive_by_cloid.get(previous_cloid) or {}
            try:
                previous_oid = int(previous.get("oid") or 0)
                previous_qty = float(previous.get("qty") or 0.0)
            except (TypeError, ValueError):
                previous_oid = 0
                previous_qty = 0.0
            record.update(
                {
                    "previous_cloid": previous_cloid,
                    "previous_oid": previous_oid,
                    "previous_qty": previous_qty,
                }
            )
        current = by_cloid.get(cloid)
        if current is None or time_ms >= int(current.get("time_ms") or 0):
            by_cloid[cloid] = record
    return list(by_cloid.values())


def _ioc_status_state(status: dict | None) -> tuple[str, float, bool, str]:
    if not isinstance(status, dict) or status.get("status") != "order":
        return "unknown", 0.0, False, ""
    outer = status.get("order") or {}
    inner = outer.get("order") or {}
    state = str(outer.get("status") or "").lower().replace("-", "").replace("_", "")
    size_known = "origSz" in inner and "sz" in inner
    try:
        filled_qty = max(
            float(inner.get("origSz") or 0.0) - float(inner.get("sz") or 0.0),
            0.0,
        )
    except (TypeError, ValueError):
        filled_qty = 0.0
        size_known = False
    cloid = str(inner.get("cloid") or "").lower()
    return state or "unknown", filled_qty, size_known, cloid


async def audit_uncovered_ioc_identities(
    market: MarketClient,
    address: str,
    identities: list[dict],
    local_rows: list[dict],
    ownership: dict,
    *,
    now_ms: int | None = None,
) -> dict:
    """Fail closed on IOC executions that may precede delayed userFills.

    An IOC identity is covered only when the same oid/cloid is present in both
    the official fill feed and the local ledger.  orderStatus is diagnostic:
    after the 300-second fill-detail grace it can prove that an execution may
    exist, but a canceled snapshot cannot prove that no execution raced it.
    """

    now_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
    local_pairs = {
        (int(row.get("oid") or 0), str(row.get("cloid") or "").lower())
        for row in local_rows
        if row.get("oid") and row.get("cloid")
    }
    local_cloids = {
        str(row.get("cloid") or "").lower()
        for row in local_rows
        if row.get("cloid")
    }
    official_pairs: set[tuple[int, str]] = set()
    official_cloids: set[str] = set()
    for row in ownership.get("bot_rows") or []:
        raw = row.get("raw") or {}
        oid = int(raw.get("oid") or 0)
        cloid = str(row.get("cloid") or "").lower()
        if oid > 0 and cloid:
            official_pairs.add((oid, cloid))
        if cloid:
            official_cloids.add(cloid)

    pending_reasons: Counter[str] = Counter()
    covered_count = 0
    clean_canceled_count = 0
    for identity in identities:
        oid = int(identity.get("oid") or 0)
        cloid = str(identity.get("cloid") or "").lower()
        covered = (
            oid > 0
            and (oid, cloid) in local_pairs
            and (oid, cloid) in official_pairs
        ) or (
            oid <= 0
            and cloid in local_cloids
            and cloid in official_cloids
        )
        if covered:
            covered_count += 1
            continue

        age_ms = now_ms - int(identity.get("time_ms") or 0)
        if age_ms < IOC_FILL_DETAIL_GRACE_MS:
            pending_reasons["fill_detail_grace"] += 1
            continue

        statuses: list[dict | None] = []
        for lookup in ([oid, cloid] if oid > 0 else [cloid]):
            try:
                statuses.append(await market.order_status(address, lookup))  # type: ignore[arg-type]
            except Exception:
                statuses.append(None)
        states = [_ioc_status_state(status) for status in statuses]
        if any(
            status_cloid and status_cloid != cloid
            for _state, _filled_qty, _size_known, status_cloid in states
        ):
            pending_reasons["identity_mismatch"] += 1
        elif any(
            state == "filled" or (size_known and filled_qty > 0)
            for state, filled_qty, size_known, _status_cloid in states
        ):
            pending_reasons["filled_without_detail"] += 1
        elif any(
            state not in {"unknown", "canceled"}
            for state, _filled_qty, _size_known, _status_cloid in states
        ):
            pending_reasons["nonterminal_or_ambiguous"] += 1
        elif any(
            state == "canceled"
            for state, _filled_qty, _size_known, _status_cloid in states
        ):
            # orderStatus reflects the surviving order object, not a complete
            # execution ledger.  Even origSz == sz cannot prove that a fill did
            # not race the cancellation and arrive later via userFills.
            pending_reasons["canceled_execution_unknown"] += 1
        else:
            pending_reasons["status_unknown"] += 1

    pending_count = sum(pending_reasons.values())
    return {
        "ioc_identity_count": len(identities),
        "ioc_covered_fill_count": covered_count,
        "ioc_clean_canceled_count": clean_canceled_count,
        "ioc_pending_count": pending_count,
        "ioc_pending_reasons": dict(pending_reasons),
    }


async def audit_uncovered_order_identities(
    market: MarketClient,
    address: str,
    identities: list[dict],
    local_rows: list[dict],
    ownership: dict,
    *,
    now_ms: int | None = None,
) -> dict:
    """Fail closed on unresolved ordinary place/modify identities after exit."""

    now_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
    local_pairs = {
        (int(row.get("oid") or 0), str(row.get("cloid") or "").lower())
        for row in local_rows
        if row.get("oid") and row.get("cloid")
    }
    local_cloids = {
        str(row.get("cloid") or "").lower()
        for row in local_rows
        if row.get("cloid")
    }
    official_pairs: set[tuple[int, str]] = set()
    official_cloids: set[str] = set()
    official_filled_qty: defaultdict[tuple[int, str], float] = defaultdict(float)
    for row in ownership.get("bot_rows") or []:
        raw = row.get("raw") or {}
        oid = int(raw.get("oid") or 0)
        cloid = str(row.get("cloid") or "").lower()
        if oid > 0 and cloid:
            official_pairs.add((oid, cloid))
        if cloid:
            official_cloids.add(cloid)
        if oid > 0 and cloid:
            try:
                official_filled_qty[(oid, cloid)] += float(raw.get("sz") or 0.0)
            except (TypeError, ValueError):
                pass

    pending_reasons: Counter[str] = Counter()
    covered_count = 0
    predecessor_resolved_count = 0
    for identity in identities:
        oid = int(identity.get("oid") or 0)
        cloid = str(identity.get("cloid") or "").lower()
        covered = (
            oid > 0
            and (oid, cloid) in local_pairs
            and (oid, cloid) in official_pairs
        ) or (
            oid <= 0
            and cloid in local_cloids
            and cloid in official_cloids
        )
        if covered:
            covered_count += 1
            continue

        previous_oid = int(identity.get("previous_oid") or 0)
        previous_cloid = str(identity.get("previous_cloid") or "").lower()
        previous_qty = float(identity.get("previous_qty") or 0.0)
        predecessor_fill = official_filled_qty.get(
            (previous_oid, previous_cloid), 0.0
        )
        if (
            identity.get("event") == "modify"
            and previous_oid > 0
            and previous_cloid
            and previous_qty > 0
            and predecessor_fill + max(previous_qty * 1e-8, 1e-9)
            >= previous_qty
        ):
            # batchModify is conditional on the predecessor.  Its full fill
            # proves the oid-less replacement was never installed, even when
            # the replacement cloid has aged out of orderStatus.
            predecessor_resolved_count += 1
            continue

        age_ms = now_ms - int(identity.get("time_ms") or 0)
        if age_ms < IOC_FILL_DETAIL_GRACE_MS:
            pending_reasons["fill_detail_grace"] += 1
            continue
        try:
            status = await market.order_status(address, cloid)  # type: ignore[arg-type]
        except Exception:
            status = None
        state, filled_qty, size_known, status_cloid = _ioc_status_state(status)
        if status_cloid and status_cloid != cloid:
            pending_reasons["identity_mismatch"] += 1
        elif state == "filled" or (size_known and filled_qty > 0):
            pending_reasons["filled_without_detail"] += 1
        elif state == "canceled":
            # As with IOC recovery, orderStatus is not a complete execution
            # ledger and canceled size fields cannot prove zero execution.
            pending_reasons["canceled_execution_unknown"] += 1
        elif state not in {"unknown"}:
            pending_reasons["nonterminal_or_ambiguous"] += 1
        else:
            pending_reasons["status_unknown"] += 1

    pending_count = sum(pending_reasons.values())
    return {
        "order_identity_count": len(identities),
        "order_covered_fill_count": covered_count,
        "order_predecessor_resolved_count": predecessor_resolved_count,
        "order_pending_count": pending_count,
        "order_pending_reasons": dict(pending_reasons),
    }


def official_position_qty(account_state: dict, accepted_coins: set[str]) -> float:
    """Return the authoritative position for exactly one configured market."""

    total = 0.0
    rows = account_state.get("assetPositions")
    if not isinstance(rows, list):
        return total
    for row in rows:
        position = row.get("position") if isinstance(row, dict) else None
        if not isinstance(position, dict) or position.get("coin") not in accepted_coins:
            continue
        total += float(position.get("szi") or 0.0)
    return total


def recovered_row(
    raw: dict,
    *,
    cloid: str,
    summary: dict,
    order_role: str | None = None,
) -> dict:
    oid = int(raw.get("oid") or 0)
    time_ms = int(raw.get("time") or 0)
    price = float(raw.get("px") or 0)
    qty = float(raw.get("sz") or 0)
    fee = float(raw.get("fee") or 0)
    pnl = float(raw.get("closedPnl") or 0)
    is_buy = str(raw.get("side") or "").upper() == "B"
    fill_id = str(raw.get("tid") or "") or hashlib.sha256(
        f"{oid}|{time_ms}|{raw.get('px')}|{raw.get('sz')}".encode()
    ).hexdigest()[:24]
    fee_model = summary.get("fee_model") or {}
    deployer_share = float(fee_model.get("deployer_share") or 0)
    own = (
        fee
        * deployer_share
        * float(fee_model.get("entropy_self_rebate_pct") or 0)
        / 100.0
    )
    referred = (
        fee
        * deployer_share
        * float(fee_model.get("entropy_referred_benefit_pct") or 0)
        / 100.0
    )
    projected_rebate = own + referred
    notional = price * qty
    recovered_role = (
        order_role
        if order_role in IOC_RECOVERY_ROLES
        else "mm:bid:0" if is_buy else "mm:ask:0"
    )
    liquidity = (
        "taker"
        if recovered_role in IOC_RECOVERY_ROLES
        else "taker" if bool(raw.get("crossed")) else "maker"
    )
    return {
        "fill_id": fill_id,
        "order_role": recovered_role,
        "cloid": cloid,
        "oid": oid,
        "is_buy": is_buy,
        "price": price,
        "qty": qty,
        "fee_usd": fee,
        "liquidity": liquidity,
        "time_ms": time_ms,
        "realized_pnl_delta": pnl,
        "projected_self_rebate": own,
        "projected_referred_benefit": referred,
        "session_id": str(summary.get("session_id") or ""),
        "mode": "live",
        "strategy": "market_maker",
        "coin": summary.get("coin"),
        "notional_usd": notional,
        "mid_at_record": None,
        "account_equity_after": float(summary.get("equity_actual") or 0),
        "projected_rebate_total_usd": projected_rebate,
        "projected_net_fee_usd": fee - projected_rebate,
        "actual_booked_wear_usd": fee - pnl,
        "projected_booked_wear_usd": fee - projected_rebate - pnl,
        "recorded_at_ms": int(time.time() * 1000),
        "recovery_source": "official_stopped_reconciliation",
    }


async def classify_official_fills(
    market: MarketClient,
    address: str,
    raw_rows: list[dict],
) -> dict:
    """Resolve each unique OID once and return privacy-safe ownership counts."""
    status_by_oid: dict[int, dict | None] = {}
    for raw in raw_rows:
        oid = int(raw.get("oid") or 0)
        if oid in status_by_oid:
            continue
        if oid <= 0:
            status_by_oid[oid] = None
            continue
        try:
            status_by_oid[oid] = await market.order_status(address, oid)  # type: ignore[arg-type]
        except Exception:
            # A transient or malformed official response is not evidence of
            # external ownership.  Preserve it as unknown and fail closed.
            status_by_oid[oid] = None

    bot_rows: list[dict] = []
    unknown_rows: list[dict] = []
    external_fill_count = 0
    unknown_fill_count = 0
    bot_oids: set[int] = set()
    external_oids: set[int] = set()
    unknown_oids: set[int] = set()
    for raw in raw_rows:
        oid = int(raw.get("oid") or 0)
        ownership, cloid = order_ownership(status_by_oid.get(oid))
        if ownership == "bot" and cloid is not None:
            bot_oids.add(oid)
            bot_rows.append(
                {
                    "raw": raw,
                    "normalized": normalized_fill(raw),
                    "cloid": cloid,
                }
            )
        elif ownership == "external":
            external_oids.add(oid)
            external_fill_count += 1
        else:
            unknown_oids.add(oid)
            unknown_fill_count += 1
            unknown_rows.append(
                {
                    "raw": raw,
                    "normalized": normalized_fill(raw),
                }
            )
    return {
        "bot_rows": bot_rows,
        "unknown_rows": unknown_rows,
        "bot_fill_count": len(bot_rows),
        "external_fill_count": external_fill_count,
        "unknown_fill_count": unknown_fill_count,
        "bot_order_count": len(bot_oids),
        "external_order_count": len(external_oids),
        "unknown_order_count": len(unknown_oids),
    }


def build_recovery_plan(
    local: list[dict],
    ownership: dict,
    *,
    max_fills: int,
) -> dict:
    """Compare the local robot ledger only with bot-owned official fills."""
    bot_rows = list(ownership.get("bot_rows") or [])
    official_counter = Counter(
        audit.fill_key(row["normalized"]) for row in bot_rows
    )
    local_counter = Counter(audit.fill_key(row) for row in local)
    # Historical sessions created before persistent cloid/order identities can
    # outlive the exchange's orderStatus lookup window.  In that case an old
    # official fill may be "unknown" even though the local bot ledger contains
    # the exact same immutable fill identity.  Treat only an exact
    # time+oid+qty+side match as legacy bot evidence; unmatched unknown rows
    # remain fail-closed.
    unknown_rows_value = ownership.get("unknown_rows")
    legacy_matched_rows: list[dict] = []
    remaining_unknown_rows: list[dict] = []
    if isinstance(unknown_rows_value, list):
        unmatched_local = local_counter - official_counter
        for row in unknown_rows_value:
            normalized = row.get("normalized") if isinstance(row, dict) else None
            if not isinstance(normalized, dict):
                remaining_unknown_rows.append(row)
                continue
            key = audit.fill_key(normalized)
            if unmatched_local[key] > 0:
                legacy_matched_rows.append(row)
                official_counter[key] += 1
                unmatched_local[key] -= 1
            else:
                remaining_unknown_rows.append(row)
    local_extra = local_counter - official_counter
    missing = official_counter - local_counter

    rows_by_key: dict[tuple, list[dict]] = defaultdict(list)
    for row in bot_rows:
        rows_by_key[audit.fill_key(row["normalized"])].append(row)
    verified: list[tuple[dict, str]] = []
    for key, count in missing.items():
        for row in rows_by_key[key][:count]:
            verified.append((row["raw"], row["cloid"]))

    missing_count = sum(missing.values())
    local_extra_count = sum(local_extra.values())
    unknown_fill_count = (
        len(remaining_unknown_rows)
        if isinstance(unknown_rows_value, list)
        else int(ownership.get("unknown_fill_count") or 0)
    )
    legacy_matched_oids = {
        int((row.get("raw") or {}).get("oid") or 0)
        for row in legacy_matched_rows
        if isinstance(row, dict)
    }
    remaining_unknown_oids = {
        int((row.get("raw") or {}).get("oid") or 0)
        for row in remaining_unknown_rows
        if isinstance(row, dict)
    }
    blocked_reasons: list[str] = []
    if unknown_fill_count:
        blocked_reasons.append("unknown_order_ownership")
    if local_extra_count:
        blocked_reasons.append("local_fill_not_in_bot_official_set")
    if missing_count > max(max_fills, 0):
        blocked_reasons.append("bot_missing_fill_limit_exceeded")

    return {
        "official_count": (
            int(ownership.get("bot_fill_count") or 0)
            + len(legacy_matched_rows)
            + int(ownership.get("external_fill_count") or 0)
            + unknown_fill_count
        ),
        "official_bot_fill_count": (
            int(ownership.get("bot_fill_count") or 0)
            + len(legacy_matched_rows)
        ),
        "legacy_matched_bot_fill_count": len(legacy_matched_rows),
        "official_external_fill_count": int(
            ownership.get("external_fill_count") or 0
        ),
        "official_unknown_fill_count": unknown_fill_count,
        "official_bot_order_count": (
            int(ownership.get("bot_order_count") or 0)
            + len({oid for oid in legacy_matched_oids if oid > 0})
        ),
        "official_external_order_count": int(
            ownership.get("external_order_count") or 0
        ),
        "official_unknown_order_count": (
            len({oid for oid in remaining_unknown_oids if oid > 0})
            if isinstance(unknown_rows_value, list)
            else int(ownership.get("unknown_order_count") or 0)
        ),
        "recoverable_count": missing_count,
        "local_extra_count": local_extra_count,
        "external_activity_detected": bool(
            int(ownership.get("external_fill_count") or 0)
        ),
        "reconciliation_ok": not unknown_fill_count
        and not local_extra_count
        and not missing_count,
        "apply_eligible": not blocked_reasons,
        "blocked_reason": blocked_reasons[0] if blocked_reasons else None,
        "blocked_reasons": blocked_reasons,
        "_verified": verified,
        "_official_counter": official_counter,
    }


async def reconcile(
    symbol: str,
    *,
    wallet_id: str = "primary",
    apply: bool,
    max_fills: int = 10,
) -> dict:
    config_path, control_path, summary_path, fills_path = paths_for(
        symbol, wallet_id
    )
    orders_path = fills_path.with_name(
        fills_path.name.removesuffix("_fills.jsonl") + "_orders.jsonl"
    )
    control = json.loads(control_path.read_text(encoding="utf-8"))
    if audit.process_alive(int(control.get("pid") or 0)):
        raise RuntimeError(f"拒绝补录：{symbol} 进程仍在运行")
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    cfg = load_config(config_path)
    credentials = resolve_live_credentials(cfg, require_private_key=False)
    account_address = recovery_account_address(credentials)
    session_id = str(summary.get("session_id") or "")
    ioc_roles = ioc_identity_roles(orders_path, session_id)
    ioc_identities = ioc_identity_records(orders_path, session_id)
    order_identities = uncertain_order_identity_records(orders_path, session_id)
    session_start = int(summary.get("session_started_at_ms") or 0)
    local = [
        row
        for row in audit.read_jsonl(fills_path)
        if str(row.get("session_id") or "") == session_id
    ]
    accepted = {cfg.market.symbol, f"{cfg.market.dex}:{cfg.market.symbol}"}
    async with aiohttp.ClientSession(trust_env=False) as session:
        market = MarketClient(cfg.market, session, info_capacity=180)
        open_raw, fill_raw, account_raw = await asyncio.gather(
            market.open_orders(account_address),
            market.fills_since(account_address, session_start),
            market.account_state(account_address),
        )
        official_orders = [row for row in open_raw if row.get("coin") in accepted]
        if official_orders:
            raise RuntimeError(f"拒绝补录：官方仍有 {symbol} 未结订单")
        official_raw = [row for row in fill_raw if row.get("coin") in accepted]
        position_qty = official_position_qty(account_raw, accepted)
        ownership = await classify_official_fills(
            market,
            account_address,
            official_raw,
        )
        plan = build_recovery_plan(local, ownership, max_fills=max_fills)
        verified: list[tuple[dict, str]] = plan["_verified"]
        if apply and not plan["apply_eligible"]:
            reason = plan["blocked_reason"]
            if reason == "unknown_order_ownership":
                raise RuntimeError(
                    "拒绝补录：存在官方成交无法确认订单归属"
                )
            if reason == "local_fill_not_in_bot_official_set":
                raise RuntimeError("拒绝补录：本地存在机器人官方集合外成交")
            if reason == "bot_missing_fill_limit_exceeded":
                raise RuntimeError(
                    "拒绝补录：机器人缺失成交 "
                    f"{plan['recoverable_count']} 笔超过单次上限 {max_fills}"
                )
            raise RuntimeError("拒绝补录：安全条件未满足")
        anticipated_local = list(local)
        if apply:
            anticipated_local.extend(
                {
                    "oid": int(raw.get("oid") or 0),
                    "cloid": cloid,
                }
                for raw, cloid in verified
            )
        ioc_audit = await audit_uncovered_ioc_identities(
            market,
            account_address,
            ioc_identities,
            anticipated_local,
            ownership,
        )
        order_audit = await audit_uncovered_order_identities(
            market,
            account_address,
            order_identities,
            anticipated_local,
            ownership,
        )
    recovered_notional = sum(
        float(raw.get("px") or 0) * float(raw.get("sz") or 0)
        for raw, _ in verified
    )
    recoverable_ioc_count = sum(
        1
        for raw, cloid in verified
        if ioc_role_for_fill(ioc_roles, int(raw.get("oid") or 0), cloid)
    )
    if apply:
        for raw, cloid in verified:
            role = ioc_role_for_fill(
                ioc_roles, int(raw.get("oid") or 0), cloid
            )
            append_jsonl(
                fills_path,
                recovered_row(
                    raw,
                    cloid=cloid,
                    summary=summary,
                    order_role=role,
                ),
            )
    final_local = [
        row
        for row in audit.read_jsonl(fills_path)
        if str(row.get("session_id") or "") == session_id
    ]
    final_counter = Counter(audit.fill_key(row) for row in final_local)
    final_reconciliation_ok = (
        not plan["official_unknown_fill_count"]
        and not (final_counter - plan["_official_counter"])
        and final_counter == plan["_official_counter"]
        and not ioc_audit["ioc_pending_count"]
        and not order_audit["order_pending_count"]
    )
    return {
        "symbol": symbol,
        "applied": apply,
        "recovered_count": len(verified) if apply else 0,
        "recoverable_count": plan["recoverable_count"],
        "recoverable_ioc_count": recoverable_ioc_count,
        **ioc_audit,
        **order_audit,
        "recovered_ioc_count": recoverable_ioc_count if apply else 0,
        "recovered_notional_usd": round(recovered_notional, 2) if apply else 0.0,
        "recoverable_notional_usd": round(recovered_notional, 2),
        "local_count_before": len(local),
        "local_count_after": len(final_local),
        "official_count": plan["official_count"],
        "official_bot_fill_count": plan["official_bot_fill_count"],
        "official_external_fill_count": plan["official_external_fill_count"],
        "official_unknown_fill_count": plan["official_unknown_fill_count"],
        "official_bot_order_count": plan["official_bot_order_count"],
        "official_external_order_count": plan["official_external_order_count"],
        "official_unknown_order_count": plan["official_unknown_order_count"],
        "local_extra_count": plan["local_extra_count"],
        "external_activity_detected": plan["external_activity_detected"],
        "reconciliation_ok": final_reconciliation_ok,
        "apply_eligible": plan["apply_eligible"],
        "blocked_reason": plan["blocked_reason"],
        "blocked_reasons": plan["blocked_reasons"],
        "official_open_orders": 0,
        "official_position_qty": position_qty,
        "official_position_zero": position_qty == 0.0,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="安全补录已停机 Entropy 机器人的官方迟到成交"
    )
    parser.add_argument("--symbol", required=True, choices=("SNDK", "ANTH"))
    parser.add_argument("--wallet-id", default="primary")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--max-fills", type=int, default=10)
    args = parser.parse_args()
    print(
        json.dumps(
            asyncio.run(
                reconcile(
                    args.symbol,
                    wallet_id=args.wallet_id,
                    apply=args.apply,
                    max_fills=max(args.max_fills, 0),
                )
            ),
            ensure_ascii=False,
            separators=(",", ":"),
        )
    )


if __name__ == "__main__":
    main()
