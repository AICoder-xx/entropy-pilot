import asyncio
import json

import pytest

from entropy_bot.broker import CloidFactory, order_ownership
from entropy_bot.credentials import LiveCredentials
from recover_stopped_fills import (
    IOC_FILL_DETAIL_GRACE_MS,
    MissingRecoveryAccountError,
    audit_uncovered_ioc_identities,
    audit_uncovered_order_identities,
    build_recovery_plan,
    classify_official_fills,
    ioc_identity_records,
    ioc_identity_roles,
    ioc_role_for_fill,
    normalized_fill,
    official_position_qty,
    recovered_row,
    recovery_account_address,
    uncertain_order_identity_records,
)


def raw_fill(oid: int, *, time_ms: int, side: str = "B") -> dict:
    return {
        "coin": "io:SNDK",
        "oid": oid,
        "time": time_ms,
        "side": side,
        "px": "100.0",
        "sz": "1.5",
        "fee": "0.1",
        "closedPnl": "0",
        "crossed": False,
    }


def order_status(cloid: str | None) -> dict:
    inner = {"origSz": "1.5", "sz": "0"}
    if cloid is not None:
        inner["cloid"] = cloid
    return {"status": "order", "order": {"status": "filled", "order": inner}}


def bot_record(raw: dict, cloid: str) -> dict:
    return {"raw": raw, "normalized": normalized_fill(raw), "cloid": cloid}


def test_recovery_account_is_required_before_official_requests():
    credentials = LiveCredentials(
        private_key=None,
        account_address=None,
        account_was_derived=False,
        source="none",
        wallet_id="primary",
    )

    with pytest.raises(MissingRecoveryAccountError, match="缺少有效账户地址"):
        recovery_account_address(credentials)

    configured = LiveCredentials(
        private_key=None,
        account_address="0x" + "1" * 40,
        account_was_derived=False,
        source="test",
        wallet_id="primary",
    )
    assert recovery_account_address(configured) == configured.account_address


def test_order_ownership_requires_valid_shape_and_bot_magic():
    bot_cloid = CloidFactory().next()
    assert order_ownership(order_status(bot_cloid)) == ("bot", bot_cloid)
    assert order_ownership(order_status("0x1234")) == ("external", None)
    assert order_ownership(order_status(None)) == ("external", None)
    assert order_ownership({"status": "unknownOid"}) == ("unknown", None)
    assert order_ownership({"status": "order", "order": {}}) == (
        "unknown",
        None,
    )


def test_classification_resolves_each_order_once_and_separates_ownership():
    bot_cloid = CloidFactory().next()
    statuses = {
        101: order_status(bot_cloid),
        202: order_status("0x1234"),
        303: None,
    }

    class FakeMarket:
        def __init__(self):
            self.calls: list[int] = []

        async def order_status(self, _address, oid):
            self.calls.append(oid)
            return statuses[oid]

    market = FakeMarket()
    rows = [
        raw_fill(101, time_ms=1),
        raw_fill(101, time_ms=2),
        raw_fill(202, time_ms=3),
        raw_fill(303, time_ms=4),
    ]
    result = asyncio.run(
        classify_official_fills(market, "redacted", rows)  # type: ignore[arg-type]
    )
    assert market.calls == [101, 202, 303]
    assert result["bot_fill_count"] == 2
    assert result["external_fill_count"] == 1
    assert result["unknown_fill_count"] == 1
    assert result["bot_order_count"] == 1
    assert result["external_order_count"] == 1
    assert result["unknown_order_count"] == 1


def test_external_fills_do_not_create_robot_ledger_mismatch():
    bot_cloid = CloidFactory().next()
    bot_raw = raw_fill(101, time_ms=1)
    local = [normalized_fill(bot_raw)]
    plan = build_recovery_plan(
        local,
        {
            "bot_rows": [bot_record(bot_raw, bot_cloid)],
            "bot_fill_count": 1,
            "external_fill_count": 30,
            "unknown_fill_count": 0,
            "bot_order_count": 1,
            "external_order_count": 15,
            "unknown_order_count": 0,
        },
        max_fills=10,
    )
    assert plan["official_count"] == 31
    assert plan["recoverable_count"] == 0
    assert plan["local_extra_count"] == 0
    assert plan["reconciliation_ok"] is True
    assert plan["apply_eligible"] is True
    assert plan["external_activity_detected"] is True


def test_recovery_limit_counts_only_bot_owned_missing_fills():
    bot_cloid = CloidFactory().next()
    bot_rows = [
        bot_record(raw_fill(100 + index, time_ms=index), bot_cloid)
        for index in range(1, 12)
    ]
    plan = build_recovery_plan(
        [],
        {
            "bot_rows": bot_rows,
            "bot_fill_count": 11,
            "external_fill_count": 40,
            "unknown_fill_count": 0,
            "bot_order_count": 11,
            "external_order_count": 20,
            "unknown_order_count": 0,
        },
        max_fills=10,
    )
    assert plan["official_count"] == 51
    assert plan["recoverable_count"] == 11
    assert plan["apply_eligible"] is False
    assert plan["blocked_reason"] == "bot_missing_fill_limit_exceeded"


def test_unknown_ownership_and_local_extra_remain_fail_closed():
    plan = build_recovery_plan(
        [normalized_fill(raw_fill(999, time_ms=9))],
        {
            "bot_rows": [],
            "bot_fill_count": 0,
            "external_fill_count": 0,
            "unknown_fill_count": 1,
            "bot_order_count": 0,
            "external_order_count": 0,
            "unknown_order_count": 1,
        },
        max_fills=10,
    )
    assert plan["apply_eligible"] is False
    assert plan["reconciliation_ok"] is False
    assert plan["blocked_reasons"] == [
        "unknown_order_ownership",
        "local_fill_not_in_bot_official_set",
    ]


def test_exact_local_fill_recovers_legacy_unknown_order_ownership():
    raw = raw_fill(999, time_ms=9)
    local = [normalized_fill(raw)]
    plan = build_recovery_plan(
        local,
        {
            "bot_rows": [],
            "unknown_rows": [{"raw": raw, "normalized": normalized_fill(raw)}],
            "bot_fill_count": 0,
            "external_fill_count": 2,
            "unknown_fill_count": 1,
            "bot_order_count": 0,
            "external_order_count": 2,
            "unknown_order_count": 1,
        },
        max_fills=10,
    )

    assert plan["legacy_matched_bot_fill_count"] == 1
    assert plan["official_count"] == 3
    assert plan["official_bot_fill_count"] == 1
    assert plan["official_unknown_fill_count"] == 0
    assert plan["official_unknown_order_count"] == 0
    assert plan["local_extra_count"] == 0
    assert plan["reconciliation_ok"] is True
    assert plan["apply_eligible"] is True
    assert plan["external_activity_detected"] is True


def test_nonmatching_legacy_unknown_fill_still_blocks():
    local_raw = raw_fill(999, time_ms=9)
    unknown_raw = raw_fill(999, time_ms=10)
    plan = build_recovery_plan(
        [normalized_fill(local_raw)],
        {
            "bot_rows": [],
            "unknown_rows": [
                {"raw": unknown_raw, "normalized": normalized_fill(unknown_raw)}
            ],
            "bot_fill_count": 0,
            "external_fill_count": 0,
            "unknown_fill_count": 1,
            "bot_order_count": 0,
            "external_order_count": 0,
            "unknown_order_count": 1,
        },
        max_fills=10,
    )

    assert plan["legacy_matched_bot_fill_count"] == 0
    assert plan["official_unknown_fill_count"] == 1
    assert plan["local_extra_count"] == 1
    assert plan["reconciliation_ok"] is False


def test_official_position_qty_is_scoped_to_configured_market():
    state = {
        "assetPositions": [
            {"position": {"coin": "io:SNDK", "szi": "0.1628"}},
            {"position": {"coin": "io:ANTH", "szi": "-0.5"}},
            {"position": {"coin": "io:SNDK", "szi": "0.0002"}},
        ]
    }

    assert official_position_qty(state, {"SNDK", "io:SNDK"}) == 0.163
    assert official_position_qty({}, {"SNDK", "io:SNDK"}) == 0.0


def test_ioc_identity_roles_require_exact_session_oid_cloid_and_unambiguous_role(
    tmp_path,
):
    cloid = CloidFactory().next()
    other_cloid = CloidFactory().next()
    action_cloid = CloidFactory().next()
    ambiguous_action_cloid = CloidFactory().next()
    rejected_inventory_cloid = CloidFactory().next()
    rejected_flatten_cloid = CloidFactory().next()
    rows = [
        {
            "session_id": "session-a",
            "event": "ioc_identity",
            "oid": 501,
            "cloid": cloid,
            "role": "risk:inventory-ioc",
        },
        {
            "session_id": "other-session",
            "event": "ioc_identity",
            "oid": 502,
            "cloid": other_cloid,
            "role": "risk:emergency-flatten",
        },
        {
            "session_id": "session-a",
            "event": "ioc_identity",
            "oid": 503,
            "cloid": other_cloid,
            "role": "mm:bid:0",
        },
        # Conflicting risk roles for one exact identity fail closed.
        {
            "session_id": "session-a",
            "event": "ioc_identity",
            "oid": 504,
            "cloid": other_cloid,
            "role": "risk:inventory-ioc",
        },
        {
            "session_id": "session-a",
            "event": "ioc_identity",
            "oid": 504,
            "cloid": other_cloid,
            "role": "risk:emergency-flatten",
        },
        # The pre-send action identity closes the crash window before an oid
        # can be persisted.
        {
            "session_id": "session-a",
            "event": "inventory_ioc",
            "cloid": action_cloid,
            "role": "risk:inventory-ioc",
        },
        # Conflicting cloid-only action evidence is discarded.
        {
            "session_id": "session-a",
            "event": "inventory_ioc",
            "cloid": ambiguous_action_cloid,
            "role": "risk:inventory-ioc",
        },
        {
            "session_id": "session-a",
            "event": "flatten",
            "cloid": ambiguous_action_cloid,
            "role": "risk:emergency-flatten",
        },
        # Strict top-level or child rejection is persisted as outcome=rejected;
        # no order was accepted, so it must not create a recovery identity.
        {
            "session_id": "session-a",
            "event": "inventory_ioc",
            "cloid": rejected_inventory_cloid,
            "role": "risk:inventory-ioc",
            "outcome": "rejected",
        },
        {
            "session_id": "session-a",
            "event": "flatten",
            "cloid": rejected_flatten_cloid,
            "role": "risk:emergency-flatten",
            "outcome": "rejected",
        },
    ]
    path = tmp_path / "orders.jsonl"
    path.write_text(
        "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
    )

    assert ioc_identity_roles(path, "session-a") == {
        (501, cloid.lower()): "risk:inventory-ioc",
        (0, action_cloid.lower()): "risk:inventory-ioc",
    }
    roles = ioc_identity_roles(path, "session-a")
    assert ioc_role_for_fill(roles, 999, action_cloid) == "risk:inventory-ioc"
    assert ioc_role_for_fill(roles, 501, cloid) == "risk:inventory-ioc"
    assert ioc_role_for_fill(roles, 999, ambiguous_action_cloid) is None
    assert ioc_role_for_fill(roles, 999, rejected_inventory_cloid) is None
    assert ioc_role_for_fill(roles, 999, rejected_flatten_cloid) is None
    assert ioc_identity_roles(tmp_path / "missing.jsonl", "session-a") == {}


@pytest.mark.parametrize("with_exact_identity", [False, True])
def test_uncovered_ioc_fill_blocks_until_detail_is_in_both_ledgers(
    tmp_path, with_exact_identity
):
    cloid = CloidFactory().next()
    rows = [
        {
            "session_id": "session-a",
            "time_ms": 1_000,
            "event": "inventory_ioc",
            "cloid": cloid,
            "role": "risk:inventory-ioc",
        }
    ]
    if with_exact_identity:
        rows.append(
            {
                "session_id": "session-a",
                "time_ms": 1_100,
                "event": "ioc_identity",
                "oid": 501,
                "cloid": cloid,
                "role": "risk:inventory-ioc",
            }
        )
    path = tmp_path / "orders.jsonl"
    path.write_text(
        "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
    )
    identities = ioc_identity_records(path, "session-a")

    class Market:
        def __init__(self):
            self.calls = []

        async def order_status(self, _address, lookup):
            self.calls.append(lookup)
            return {
                "status": "order",
                "order": {
                    "status": "filled",
                    "oid": 501,
                    "order": {
                        "oid": 501,
                        "cloid": cloid,
                        "origSz": "1.5",
                        "sz": "0",
                    },
                },
            }

    market = Market()
    empty = asyncio.run(
        audit_uncovered_ioc_identities(
            market,  # type: ignore[arg-type]
            "redacted",
            identities,
            [],
            {"bot_rows": []},
            now_ms=1_100 + IOC_FILL_DETAIL_GRACE_MS,
        )
    )
    assert empty["ioc_pending_count"] == 1
    assert empty["ioc_pending_reasons"] == {"filled_without_detail": 1}

    raw = raw_fill(501, time_ms=1_050)
    local = {**normalized_fill(raw), "cloid": cloid}
    ownership = {
        "bot_rows": [
            {"raw": raw, "normalized": normalized_fill(raw), "cloid": cloid}
        ]
    }
    covered = asyncio.run(
        audit_uncovered_ioc_identities(
            market,  # type: ignore[arg-type]
            "redacted",
            identities,
            [local],
            ownership,
            now_ms=1_100 + IOC_FILL_DETAIL_GRACE_MS,
        )
    )
    assert covered["ioc_pending_count"] == 0
    assert covered["ioc_covered_fill_count"] == 1


@pytest.mark.parametrize(
    ("audit", "pending_key", "reason_key"),
    [
        (audit_uncovered_ioc_identities, "ioc_pending_count", "ioc_pending_reasons"),
        (
            audit_uncovered_order_identities,
            "order_pending_count",
            "order_pending_reasons",
        ),
    ],
)
def test_fill_coverage_requires_the_same_exact_oid_cloid_pair(
    audit, pending_key, reason_key
):
    target_cloid = CloidFactory().next()
    other_cloid = CloidFactory().next()
    target_oid = 501
    other_oid = 502
    identity = {
        "oid": target_oid,
        "cloid": target_cloid,
        "role": "risk:inventory-ioc",
        "event": "inventory_ioc",
        "outcome": "submitted",
        "time_ms": 1_000,
    }

    # Each component exists in both ledgers, but only in crossed identities:
    # (target oid, other cloid) and (other oid, target cloid).  Independent
    # oid/cloid set membership must never be mistaken for exact coverage.
    local_rows = [
        {
            **normalized_fill(raw_fill(target_oid, time_ms=1_100)),
            "cloid": other_cloid,
        },
        {
            **normalized_fill(raw_fill(other_oid, time_ms=1_101)),
            "cloid": target_cloid,
        },
    ]
    ownership = {
        "bot_rows": [
            bot_record(raw_fill(target_oid, time_ms=1_100), other_cloid),
            bot_record(raw_fill(other_oid, time_ms=1_101), target_cloid),
        ]
    }

    class Market:
        async def order_status(self, _address, _lookup):
            return {
                "status": "order",
                "order": {
                    "status": "filled",
                    "oid": target_oid,
                    "order": {
                        "oid": target_oid,
                        "cloid": target_cloid,
                        "origSz": "1",
                        "sz": "0",
                    },
                },
            }

    result = asyncio.run(
        audit(
            Market(),  # type: ignore[arg-type]
            "redacted",
            [identity],
            local_rows,
            ownership,
            now_ms=1_000 + IOC_FILL_DETAIL_GRACE_MS,
        )
    )
    assert result[pending_key] == 1
    assert result[reason_key] == {"filled_without_detail": 1}


@pytest.mark.parametrize(
    ("action_outcome", "with_exact_identity"),
    [("submitted", False), ("send_uncertain", False), ("submitted", True)],
)
def test_uncovered_cancel_remains_pending_after_full_detail_grace(
    tmp_path, action_outcome, with_exact_identity
):
    cloid = CloidFactory().next()
    path = tmp_path / "orders.jsonl"
    rows = [
        {
            "session_id": "session-a",
            "time_ms": 1_000,
            "event": "flatten",
            "cloid": cloid,
            "role": "risk:emergency-flatten",
            "outcome": action_outcome,
        }
    ]
    if with_exact_identity:
        rows.append(
            {
                "session_id": "session-a",
                "time_ms": 1_100,
                "event": "ioc_identity",
                "oid": 701,
                "cloid": cloid,
                "role": "risk:emergency-flatten",
            }
        )
    path.write_text(
        "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
    )
    identities = ioc_identity_records(path, "session-a")
    identity_time_ms = 1_100 if with_exact_identity else 1_000

    class Market:
        async def order_status(self, _address, _lookup):
            return {
                "status": "order",
                "order": {
                    "status": "canceled",
                    "order": {"cloid": cloid, "origSz": "1", "sz": "1"},
                },
            }

    pending = asyncio.run(
        audit_uncovered_ioc_identities(
            Market(),  # type: ignore[arg-type]
            "redacted",
            identities,
            [],
            {"bot_rows": []},
            now_ms=identity_time_ms + IOC_FILL_DETAIL_GRACE_MS - 1,
        )
    )
    still_pending = asyncio.run(
        audit_uncovered_ioc_identities(
            Market(),  # type: ignore[arg-type]
            "redacted",
            identities,
            [],
            {"bot_rows": []},
            now_ms=identity_time_ms + IOC_FILL_DETAIL_GRACE_MS,
        )
    )
    assert pending["ioc_pending_reasons"] == {"fill_detail_grace": 1}
    assert still_pending["ioc_pending_count"] == 1
    assert still_pending["ioc_pending_reasons"] == {
        "canceled_execution_unknown": 1
    }
    assert still_pending["ioc_clean_canceled_count"] == 0


def test_uncertain_place_modify_identity_records_ignore_resolved_and_rejected(
    tmp_path,
):
    place_cloid = CloidFactory().next()
    modify_cloid = CloidFactory().next()
    missing_oid_cloid = CloidFactory().next()
    resolved_cloid = CloidFactory().next()
    ignored_cloid = CloidFactory().next()
    rows = [
        {
            "session_id": "session-a",
            "time_ms": 100,
            "event": "place",
            "role": "mm:bid:0",
            "cloid": place_cloid,
            "outcome": "send_uncertain",
        },
        {
            "session_id": "session-a",
            "time_ms": 200,
            "event": "modify",
            "role": "mm:ask:0",
            "cloid": modify_cloid,
            # This is the previous oid and must not be assigned to the
            # replacement cloid during stopped recovery.
            "oid": 777,
            "outcome": "submitted",
        },
        {
            "session_id": "session-a",
            "time_ms": 300,
            "event": "place",
            "role": "mm:bid:0",
            "cloid": missing_oid_cloid,
            "outcome": "resting",
        },
        {
            "session_id": "session-a",
            "time_ms": 400,
            "event": "place",
            "role": "mm:bid:0",
            "cloid": resolved_cloid,
            "outcome": "send_uncertain",
        },
        {
            "session_id": "session-a",
            "time_ms": 450,
            "event": "order_identity",
            "role": "mm:bid:0",
            "cloid": resolved_cloid,
            "oid": 889,
        },
        {
            "session_id": "session-a",
            "time_ms": 500,
            "event": "place",
            "role": "mm:ask:0",
            "cloid": ignored_cloid,
            "outcome": "rejected",
        },
        {
            "session_id": "session-a",
            "time_ms": 600,
            "event": "place",
            "role": "mm:ask:0",
            "cloid": CloidFactory().next(),
            "oid": 888,
            "outcome": "resting",
        },
        # Legacy rows did not have outcome and are not retroactively treated as
        # crash-window evidence.
        {
            "session_id": "session-a",
            "time_ms": 700,
            "event": "place",
            "role": "mm:ask:0",
            "cloid": CloidFactory().next(),
        },
    ]
    path = tmp_path / "orders.jsonl"
    path.write_text(
        "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
    )

    records = uncertain_order_identity_records(path, "session-a")
    assert {row["cloid"] for row in records} == {
        place_cloid.lower(),
        modify_cloid.lower(),
        missing_oid_cloid.lower(),
    }
    assert all(row["oid"] == 0 for row in records)


def test_uncertain_ordinary_order_blocks_restart_until_fill_is_covered():
    cloid = CloidFactory().next()
    identity = {
        "oid": 0,
        "cloid": cloid,
        "role": "mm:bid:0",
        "event": "place",
        "outcome": "send_uncertain",
        "time_ms": 1_000,
    }

    class CanceledMarket:
        async def order_status(self, _address, _lookup):
            return {
                "status": "order",
                "order": {
                    "status": "canceled",
                    "order": {"cloid": cloid, "origSz": "1", "sz": "1"},
                },
            }

    pending = asyncio.run(
        audit_uncovered_order_identities(
            CanceledMarket(),  # type: ignore[arg-type]
            "redacted",
            [identity],
            [],
            {"bot_rows": []},
            now_ms=1_000 + IOC_FILL_DETAIL_GRACE_MS,
        )
    )
    assert pending["order_pending_count"] == 1
    assert pending["order_pending_reasons"] == {
        "canceled_execution_unknown": 1
    }

    raw = raw_fill(909, time_ms=1_100)
    local = {**normalized_fill(raw), "cloid": cloid}
    ownership = {"bot_rows": [bot_record(raw, cloid)]}
    covered = asyncio.run(
        audit_uncovered_order_identities(
            CanceledMarket(),  # type: ignore[arg-type]
            "redacted",
            [identity],
            [local],
            ownership,
            now_ms=1_000 + IOC_FILL_DETAIL_GRACE_MS,
        )
    )
    assert covered["order_pending_count"] == 0
    assert covered["order_covered_fill_count"] == 1


def test_uncertain_modify_is_resolved_by_complete_predecessor_fill():
    previous_cloid = CloidFactory().next()
    replacement_cloid = CloidFactory().next()
    identity = {
        "oid": 0,
        "cloid": replacement_cloid,
        "role": "mm:bid:0",
        "event": "modify",
        "outcome": "send_uncertain",
        "time_ms": 1_000,
        "previous_oid": 910,
        "previous_cloid": previous_cloid,
        "previous_qty": 1.0,
    }
    raw = raw_fill(910, time_ms=1_100)
    raw["sz"] = "1.0"
    ownership = {"bot_rows": [bot_record(raw, previous_cloid)]}

    class UnknownMarket:
        async def order_status(self, _address, _lookup):
            return {"status": "unknownOid"}

    result = asyncio.run(
        audit_uncovered_order_identities(
            UnknownMarket(),  # type: ignore[arg-type]
            "redacted",
            [identity],
            [],
            ownership,
            now_ms=1_000 + IOC_FILL_DETAIL_GRACE_MS,
        )
    )

    assert result["order_pending_count"] == 0
    assert result["order_predecessor_resolved_count"] == 1


def test_recovered_ioc_fill_keeps_risk_role_and_is_always_taker():
    cloid = CloidFactory().next()
    raw = raw_fill(601, time_ms=123, side="A")
    # Exercise the safety override even when an old/raw payload omitted the
    # crossed flag or reported it incorrectly.
    raw["crossed"] = False
    summary = {
        "session_id": "session-a",
        "coin": "io:SNDK",
        "equity_actual": 1000.0,
        "fee_model": {
            "deployer_share": 0.5,
            "entropy_self_rebate_pct": 160.0,
            "entropy_referred_benefit_pct": 0.0,
        },
    }

    recovered = recovered_row(
        raw,
        cloid=cloid,
        summary=summary,
        order_role="risk:emergency-flatten",
    )
    assert recovered["order_role"] == "risk:emergency-flatten"
    assert recovered["liquidity"] == "taker"

    # Backward compatibility for ledgers created before ioc_identity existed.
    legacy = recovered_row(raw, cloid=cloid, summary=summary)
    assert legacy["order_role"] == "mm:ask:0"
    assert legacy["liquidity"] == "maker"
