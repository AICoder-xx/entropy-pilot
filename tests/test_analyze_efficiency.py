import json

import pytest

from analyze_efficiency import analyze, main, render_table


def test_unknown_fee_is_not_reported_as_zero(tmp_path):
    _write_jsonl(tmp_path / "live_market_maker_io_TEST_fills.jsonl", [{
        "fill_id": "unknown-fee", "coin": "io:TEST", "session_id": "s",
        "notional_usd": 10, "fee_usd": None, "projected_net_fee_usd": None}])
    result = analyze(tmp_path)["totals"]["metrics"]
    assert result["gross_fee_usd"] is None
    assert result["projected_net_fee_usd"] is None
    assert result["missing_fee_count"] == 1


def test_named_wallet_ledgers_are_included_without_merging_equal_session_ids(tmp_path):
    for directory in [tmp_path, tmp_path / "wallets" / "a", tmp_path / "wallets" / "b"]:
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "live_market_maker_io_TEST_fills.jsonl").write_text(json.dumps({
            "session_id": "same", "coin": "io:TEST", "fill_id": "same-fill",
            "notional_usd": 10, "fee_usd": 0.01, "mode": "live"}) + "\n")
    report = analyze(tmp_path)
    assert report["totals"]["metrics"]["fill_count"] == 3
    assert len(report["sessions"]) == 3
    assert len(report["inputs"]["files_read"]) == 3
    assert {r["wallet_scope"] for r in report["sessions"]} == {".", "wallets/a", "wallets/b"}


def _write_jsonl(path, rows):
    path.write_text(
        "".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows),
        encoding="utf-8",
    )


def _fixture_state(tmp_path):
    prefix = "live_market_maker_io_TEST"
    fills = [
        {
            "fill_id": "b0",
            "session_id": "old-session",
            "coin": "io:TEST",
            "mode": "live",
            "order_role": "mm:bid:0",
            "is_buy": True,
            "time_ms": 1,
            "notional_usd": 100.0,
            "fee_usd": 0.03,
            "projected_rebate_total_usd": 0.024,
        },
        {
            "fill_id": "b1-near",
            "session_id": "old-session",
            "coin": "io:TEST",
            "mode": "live",
            "order_role": "mm:bid:1",
            "is_buy": True,
            "time_ms": 501,
            "notional_usd": 200.0,
            "fee_usd": 0.06,
            "projected_net_fee_usd": 0.012,
        },
        {
            "fill_id": "b1-far",
            "session_id": "old-session",
            "coin": "io:TEST",
            "mode": "live",
            "order_role": "mm:bid:1",
            "is_buy": True,
            "time_ms": 8_001,
            "notional_usd": 50.0,
            "fee_usd": 0.015,
            "projected_net_fee_usd": 0.003,
        },
        {
            "fill_id": "s0",
            "session_id": "old-session",
            "coin": "io:TEST",
            "mode": "live",
            "order_role": "mm:ask:0",
            "is_buy": False,
            "time_ms": 20_000,
            "notional_usd": 300.0,
            "fee_usd": 0.09,
            "projected_net_fee_usd": 0.018,
        },
        {
            "fill_id": "s1",
            "session_id": "old-session",
            "coin": "io:TEST",
            "mode": "live",
            "order_role": "mm:ask:1",
            "is_buy": False,
            "time_ms": 24_000,
            "notional_usd": 400.0,
            "fee_usd": 0.12,
            "projected_net_fee_usd": 0.024,
        },
    ]
    markouts = [
        {
            "fill_id": "b0",
            "session_id": "old-session",
            "horizon_sec": 30,
            "markout_pnl_usd": -1.0,
            # Deliberately omit notional: the analyzer must join the fill.
            "observed_time_ms": 31_000,
        },
        {
            "fill_id": "b1-near",
            "session_id": "old-session",
            "horizon_sec": 1,
            "notional_usd": 200.0,
            "markout_pnl_usd": -99.0,
        },
        {
            "fill_id": "b1-near",
            "session_id": "old-session",
            "horizon_sec": 30,
            "notional_usd": 200.0,
            "markout_pnl_usd": 1.0,
            "observed_time_ms": 31_501,
        },
        {
            "fill_id": "s0",
            "session_id": "old-session",
            "horizon_sec": 30,
            "notional_usd": 300.0,
            "markout_pnl_usd": -3.0,
            "observed_time_ms": 50_000,
        },
    ]
    orders = [
        {
            "session_id": "old-session",
            "event": "place",
            "role": "mm:bid:0",
            "is_buy": True,
        },
        {
            "session_id": "old-session",
            "event": "modify",
            "role": "mm:bid:0",
            "is_buy": True,
        },
        {
            "session_id": "old-session",
            "event": "place",
            "role": "mm:bid:1",
            "is_buy": True,
        },
        {
            "session_id": "old-session",
            "event": "modify_rejected",
            "role": "mm:bid:1",
            "is_buy": True,
        },
        {
            "session_id": "old-session",
            "event": "cancel",
            "role": "mm:bid:1",
            "is_buy": True,
            "cloid": "0x" + "a" * 32,
        },
        {
            "session_id": "old-session",
            "event": "cancel",
            "role": "mm:bid:1",
            "is_buy": True,
            "cloid": "0x" + "a" * 32,
        },
        {
            "session_id": "old-session",
            "event": "observation",
            "role": "mm:bid:1",
            "is_buy": True,
        },
        {
            "session_id": "old-session",
            "event": "inventory_ioc",
            "role": "risk:inventory-ioc",
            "is_buy": False,
        },
        {
            "session_id": "old-session",
            "event": "flatten",
            "role": "risk:emergency-flatten",
            "is_buy": False,
        },
        {
            "session_id": "old-session",
            "event": "ioc_identity",
            "role": "risk:emergency-flatten",
            "is_buy": False,
            "oid": 99,
        },
        {
            "session_id": "old-session",
            "event": "order_identity",
            "role": "mm:bid:0",
            "is_buy": True,
            "oid": 100,
        },
    ]
    _write_jsonl(tmp_path / f"{prefix}_fills.jsonl", fills)
    _write_jsonl(tmp_path / f"{prefix}_markouts.jsonl", markouts)
    _write_jsonl(tmp_path / f"{prefix}_orders.jsonl", orders)
    (tmp_path / f"{prefix}_summary.json").write_text(
        json.dumps(
            {
                "session_id": "old-session",
                "coin": "io:TEST",
                "status": "stopped",
                "volume_usd": 1050.0,
                "fill_count": 5,
                "requote_stats": {"modified": 1, "replaced": 0},
                "action_budget": {"local_spent": 2, "used_official": 12},
            }
        ),
        encoding="utf-8",
    )


def test_analyze_groups_actions_markout_and_summary_without_today_filter(tmp_path):
    _fixture_state(tmp_path)

    report = analyze(tmp_path)

    # time_ms=1 proves the old hard-coded "today" boundary is gone.
    assert report["totals"]["metrics"]["fill_count"] == 5
    session = report["sessions"][0]
    assert session["session_id"] == "old-session"
    assert session["market"] == "io:TEST"
    assert session["metrics"]["fill_notional_usd"] == 1050.0
    assert session["metrics"]["order_action_count"] == 7
    assert session["metrics"]["volume_usd_per_action"] == 150.0
    assert session["metrics"]["fills_per_1000_actions"] == pytest.approx(
        714.2857142857
    )
    assert session["metrics"]["markout_30_notional_usd"] == 600.0
    assert session["metrics"]["weighted_markout_30s_bps"] == -50.0
    # Gross adverse wear is $1 + $3; the favorable $1 is not netted against it.
    assert session["metrics"]["adverse_wear_30s_usd"] == 4.0
    assert abs(session["metrics"]["adverse_wear_30s_bps"] - 66.6666666667) < 1e-9
    assert report["inputs"]["diagnostics"]["unknown_order_event_rows"] == 1
    assert report["inputs"]["diagnostics"]["duplicate_cancel_action_rows"] == 1
    comparison = session["summary_snapshots"][0]["ledger_comparison"]
    assert comparison == {
        "fill_notional_delta_usd": 0.0,
        "fill_count_delta": 0,
        "modify_count_delta": 0,
    }

    bid0 = next(group for group in session["groups"] if group["role"] == "mm:bid:0")
    assert bid0["side"] == "buy"
    assert bid0["level"] == 0
    assert bid0["metrics"]["fill_count"] == 1
    assert bid0["metrics"]["order_action_count"] == 2
    assert bid0["metrics"]["fills_per_1000_actions"] == 500.0
    assert bid0["metrics"]["volume_usd_per_action"] == 50.0
    risk_actions = {
        group["role"]: group["metrics"]["order_action_count"]
        for group in session["groups"]
        if group["role"].startswith("risk:")
    }
    assert risk_actions == {
        "risk:emergency-flatten": 1,
        "risk:inventory-ioc": 1,
    }


def test_cancel_retries_count_by_attempt_while_legacy_polls_stay_deduped(tmp_path):
    prefix = "live_market_maker_io_TEST"
    cloid = "0x" + "d" * 32
    base = {
        "session_id": "retry-session",
        "coin": "io:TEST",
        "event": "cancel",
        "role": "mm:bid:0",
        "is_buy": True,
        "cloid": cloid,
    }
    _write_jsonl(
        tmp_path / f"{prefix}_orders.jsonl",
        [
            dict(base),
            dict(base),  # historical reconciliation-poll duplicate
            {**base, "attempt": 2, "reason": "official_still_open_retry"},
            {**base, "attempt": 2, "reason": "official_still_open_retry"},
            {**base, "attempt": 3, "reason": "official_still_open_retry"},
        ],
    )

    report = analyze(tmp_path)
    session = report["sessions"][0]
    assert session["metrics"]["order_action_count"] == 3
    assert session["metrics"]["order_events"] == {"cancel": 3}
    assert report["inputs"]["diagnostics"]["duplicate_cancel_action_rows"] == 2


def test_same_side_level_sweep_uses_level1_as_denominator(tmp_path):
    _fixture_state(tmp_path)

    sweep = analyze(tmp_path)["sessions"][0]["same_side_level_sweeps"]
    buy = next(row for row in sweep["by_side"] if row["side"] == "buy")
    sell = next(row for row in sweep["by_side"] if row["side"] == "sell")

    assert buy["eligible_level1_fills"] == 2
    assert buy["within_1s_count"] == 1
    assert buy["within_1s_count_pct"] == 50.0
    assert buy["within_5s_count_pct"] == 50.0
    assert sell["eligible_level1_fills"] == 1
    assert sell["within_1s_count_pct"] == 0.0
    assert sell["within_5s_count_pct"] == 100.0


def test_filters_and_chinese_table(tmp_path):
    _fixture_state(tmp_path)

    assert analyze(tmp_path, symbols=["OTHER"])["sessions"] == []
    assert analyze(tmp_path, session_ids=["other-session"])["sessions"] == []
    report = analyze(tmp_path, symbols=["io:TEST"], session_ids=["old-session"])
    table = render_table(report)
    assert "历史做市效率" in table
    assert "不利磨损" in table
    assert "mm:bid:0" in table
    assert "1秒同扫" in table


def test_cli_can_emit_machine_readable_json(tmp_path, capsys):
    _fixture_state(tmp_path)

    assert main(["--state-dir", str(tmp_path), "--symbols", "TEST", "--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["sessions"][0]["market"] == "io:TEST"
    assert payload["totals"]["metrics"]["fill_count"] == 5
