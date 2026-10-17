from recover_stopped_orders import build_recovery_plan


def test_order_recovery_plan_allows_only_stopped_bot_orders():
    plan = build_recovery_plan(
        process_running=False,
        ownerships=["bot", "bot"],
        max_orders=10,
    )

    assert plan["apply_eligible"] is True
    assert plan["official_bot_open_orders"] == 2
    assert plan["blocked_reasons"] == []


def test_order_recovery_plan_blocks_running_process():
    plan = build_recovery_plan(
        process_running=True,
        ownerships=["bot"],
        max_orders=10,
    )

    assert plan["apply_eligible"] is False
    assert plan["blocked_reason"] == "process_still_running"


def test_order_recovery_plan_blocks_external_and_unknown_orders():
    plan = build_recovery_plan(
        process_running=False,
        ownerships=["bot", "external", "unknown"],
        max_orders=10,
    )

    assert plan["apply_eligible"] is False
    assert plan["blocked_reasons"] == [
        "external_open_order_present",
        "unknown_open_order_ownership",
    ]


def test_order_recovery_plan_enforces_order_limit():
    plan = build_recovery_plan(
        process_running=False,
        ownerships=["bot"] * 11,
        max_orders=10,
    )

    assert plan["apply_eligible"] is False
    assert plan["blocked_reason"] == "bot_open_order_limit_exceeded"
