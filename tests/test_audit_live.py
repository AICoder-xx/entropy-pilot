from __future__ import annotations

import os
from datetime import datetime

from audit_live import (
    _market_paths,
    _windows_process_alive,
    evaluate_order_confirmation,
    fill_snapshot_timestamp,
    latest_account_equity,
    markout_metrics,
    official_snapshot_issue,
    process_alive,
    reconcile_fills,
    sanitize_log_line,
    scan_run_log,
    select_current_run_summary,
)


def test_named_wallet_audit_paths_follow_runtime_scope():
    fills, markouts, log, runtime = _market_paths("SNDK", "wallet1")
    assert fills.as_posix().endswith(
        "state/wallets/wallet1/live_market_maker_io_SNDK_fills.jsonl"
    )
    assert markouts.as_posix().endswith(
        "state/wallets/wallet1/live_market_maker_io_SNDK_markouts.jsonl"
    )
    assert log.as_posix().endswith("logs/wallets/wallet1/sndk/entropy_bot.log")
    assert runtime.name == "config.live.ui.wallet1.sndk.runtime.json"


def test_intentional_official_rest_yield_is_not_a_warning():
    assert official_snapshot_issue(
        rest_deferred=True,
        live_ok=False,
        updated_at_ms=0,
        age_sec=None,
        max_age_sec=60,
    ) is None


def test_direct_official_audit_still_fails_on_missing_snapshot():
    assert official_snapshot_issue(
        rest_deferred=False,
        live_ok=False,
        updated_at_ms=0,
        age_sec=None,
        max_age_sec=60,
    ) == {"severity": "error", "code": "official_snapshot_missing"}


def test_shared_account_equity_uses_freshest_market_heartbeat():
    results = {
        "SNDK": {
            "summary": {"updated_at_ms": 100},
            "official": {"updated_at_ms": 0, "equity_usd": 5990.0},
        },
        "ANTH": {
            "summary": {"updated_at_ms": 200},
            "official": {"updated_at_ms": 0, "equity_usd": 5995.0},
        },
    }

    assert latest_account_equity(results) == 5995.0


def test_windows_process_alive_checks_exit_code_not_only_handle_presence():
    class FakeKernel32:
        def __init__(self, exit_code):
            self.exit_code = exit_code
            self.closed = False

        def OpenProcess(self, _access, _inherit, _pid):
            return 123

        def GetExitCodeProcess(self, _handle, pointer):
            pointer._obj.value = self.exit_code
            return 1

        def CloseHandle(self, _handle):
            self.closed = True

    exited = FakeKernel32(0)
    active = FakeKernel32(259)
    assert _windows_process_alive(1, exited) is False
    assert exited.closed is True
    assert _windows_process_alive(1, active) is True
    assert active.closed is True


def test_fill_reconciliation_uses_independent_low_rate_cache_timestamp():
    assert fill_snapshot_timestamp(
        {"updated_at_ms": 2_000_000, "fills_updated_at_ms": 1_500_000}
    ) == 1_500_000
    assert fill_snapshot_timestamp(
        {"updated_at_ms": 2_000_000, "recent_fills": [{"time_ms": 1_400_000}]},
        finality_sec=60,
    ) == 1_460_000
    assert fill_snapshot_timestamp({"updated_at_ms": 2_000_000}) == 0


def test_new_control_run_rejects_previous_session_summary():
    summary = {
        "session_started_at_ms": 1_000_000,
        "status": "halted",
        "position_qty": 9.0,
    }
    controller = {
        "desired_state": "running",
        "started_at_ms": 2_000_000,
    }

    selected, current, effective_start = select_current_run_summary(
        summary, controller
    )

    assert selected == {}
    assert current is False
    assert effective_start == 2_000_000


def test_current_control_run_keeps_its_summary():
    summary = {
        "session_started_at_ms": 2_001_000,
        "status": "running",
        "position_qty": 0.0,
    }
    controller = {
        "desired_state": "running",
        "started_at_ms": 2_000_000,
    }

    selected, current, effective_start = select_current_run_summary(
        summary, controller
    )

    assert selected is summary
    assert current is True
    assert effective_start == 2_001_000


def _confirmation_point(
    *, updated_at_ms: int, official: int, expected: int, fills: int
) -> dict[str, int]:
    return {
        "official_updated_at_ms": updated_at_ms,
        "official_open_orders": official,
        "expected_open_orders": expected,
        "fill_count": fills,
    }


def test_order_confirmation_treats_one_sided_snapshot_as_active_turnover():
    result, issue = evaluate_order_confirmation(
        _confirmation_point(updated_at_ms=100, official=1, expected=2, fills=10),
        _confirmation_point(updated_at_ms=200, official=1, expected=2, fills=11),
    )

    assert result["classification"] == "active_turnover"
    assert result["still_below_expected"] is True
    assert issue is None


def test_order_confirmation_warns_when_one_side_stays_missing_without_turnover():
    result, issue = evaluate_order_confirmation(
        _confirmation_point(updated_at_ms=100, official=1, expected=2, fills=10),
        _confirmation_point(updated_at_ms=200, official=1, expected=2, fills=10),
    )

    assert result["classification"] == "needs_recheck"
    assert result["still_below_expected"] is True
    assert issue == "official_one_sided_orders_needs_recheck"


def test_order_confirmation_clears_when_second_side_is_restored():
    result, issue = evaluate_order_confirmation(
        _confirmation_point(updated_at_ms=100, official=1, expected=2, fills=10),
        _confirmation_point(updated_at_ms=200, official=2, expected=2, fills=10),
    )

    assert result["classification"] == "restored"
    assert result["still_below_expected"] is False
    assert issue is None


def test_order_confirmation_preserves_zero_order_warning():
    result, issue = evaluate_order_confirmation(
        _confirmation_point(updated_at_ms=100, official=0, expected=2, fills=10),
        _confirmation_point(updated_at_ms=200, official=0, expected=2, fills=10),
    )

    assert result["classification"] == "needs_recheck"
    assert result["still_zero"] is True
    assert issue == "official_zero_orders_needs_recheck"


def test_order_confirmation_warns_when_zero_order_snapshot_does_not_advance():
    result, issue = evaluate_order_confirmation(
        _confirmation_point(updated_at_ms=100, official=0, expected=2, fills=10),
        _confirmation_point(updated_at_ms=100, official=0, expected=2, fills=10),
    )

    assert result["classification"] == "snapshot_not_advanced"
    assert result["still_zero"] is True
    assert issue == "official_zero_orders_snapshot_not_advanced"


def test_reconcile_fills_uses_rolling_finality_window_and_preserves_partials():
    local = [
        {
            "session_id": "s1",
            "time_ms": 1_100_000,
            "oid": 7,
            "qty": "0.0010",
            "is_buy": True,
        },
        {
            "session_id": "s1",
            "time_ms": 1_100_000,
            "oid": 7,
            "qty": "0.009",
            "is_buy": True,
        },
        {
            "session_id": "s1",
            "time_ms": 1_990_000,
            "oid": 8,
            "qty": "0.01",
            "is_buy": False,
        },
    ]
    official = [
        {"time_ms": 1_100_000, "oid": 7, "qty": 0.001, "is_buy": True},
        {"time_ms": 1_100_000, "oid": 7, "qty": 0.009, "is_buy": True},
        {"time_ms": 1_990_000, "oid": 8, "qty": 0.01, "is_buy": False},
    ]

    result = reconcile_fills(
        local,
        official,
        session_id="s1",
        session_started_at_ms=1_000_000,
        official_updated_at_ms=2_000_000,
        finality_sec=60,
        window_sec=900,
    )

    assert result["local_count"] == 2
    assert result["official_count"] == 2
    assert result["missing_local"] == 0
    assert result["extra_local"] == 0
    assert result["ok"] is True


def test_reconcile_fills_reports_missing_local_fill():
    result = reconcile_fills(
        [],
        [{"time_ms": 1_100_000, "oid": 9, "qty": "0.01", "is_buy": False}],
        session_id="s1",
        session_started_at_ms=1_000_000,
        official_updated_at_ms=2_000_000,
        finality_sec=60,
        window_sec=900,
    )

    assert result["missing_local"] == 1
    assert result["ok"] is False


def test_reconcile_fills_excludes_classified_external_activity():
    result = reconcile_fills(
        [],
        [
            {
                "time_ms": 1_100_000,
                "oid": 9,
                "qty": "0.01",
                "is_buy": False,
                "ownership": "external",
            }
        ],
        session_id="s1",
        session_started_at_ms=1_000_000,
        official_updated_at_ms=2_000_000,
        finality_sec=60,
        window_sec=900,
    )

    assert result["official_count"] == 1
    assert result["official_bot_count"] == 0
    assert result["official_external_count"] == 1
    assert result["missing_local"] == 0
    assert result["ok"] is True


def test_reconcile_fills_fails_closed_on_unknown_ownership():
    result = reconcile_fills(
        [],
        [
            {
                "time_ms": 1_100_000,
                "oid": 9,
                "qty": "0.01",
                "is_buy": False,
                "ownership": "unknown",
            }
        ],
        session_id="s1",
        session_started_at_ms=1_000_000,
        official_updated_at_ms=2_000_000,
        finality_sec=60,
        window_sec=900,
    )

    assert result["official_unknown_count"] == 1
    assert result["missing_local"] == 0
    assert result["ok"] is False


def test_markout_metrics_are_notional_weighted_and_baseline_scoped():
    fills = [
        {"fill_id": "old", "time_ms": 99, "notional_usd": "100"},
        {"fill_id": "a", "time_ms": 100, "notional_usd": "100"},
        {"fill_id": "b", "time_ms": 101, "notional_usd": "300"},
    ]
    markouts = [
        {
            "fill_id": "old",
            "fill_time_ms": 99,
            "horizon_sec": 30,
            "markout_pnl_usd": "99",
            "projected_execution_wear_usd": "99",
        },
        {
            "fill_id": "a",
            "fill_time_ms": 100,
            "horizon_sec": 30,
            "markout_pnl_usd": "0.01",
            "projected_execution_wear_usd": "0.001",
        },
        {
            "fill_id": "b",
            "fill_time_ms": 101,
            "horizon_sec": 30,
            "markout_pnl_usd": "-0.03",
            "projected_execution_wear_usd": "0.003",
        },
    ]

    result = markout_metrics(
        fills, markouts, baseline_ms=100, minimum_samples=20
    )

    assert result["samples"] == 2
    assert result["observed_notional_usd"] == 400.0
    assert abs(result["weighted_markout_bps"] + 0.5) < 1e-12
    assert abs(result["projected_execution_wear_bps"] - 0.1) < 1e-12
    assert result["sample_gate_met"] is False


def test_scan_run_log_ignores_previous_run_and_ages_pending(tmp_path):
    log = tmp_path / "bot.log"
    log.write_text(
        "2026-08-28 12:00:00 ERROR old run\n"
        "Traceback old\n"
        "2026-08-28 13:00:00 INFO new run\n"
        "2026-08-28 13:00:10 WARNING 订单状态等待对账 0/60 秒\n"
        "2026-08-28 13:00:11 INFO 成交已到达\n",
        encoding="utf-8",
    )
    started = int(datetime(2026, 8, 28, 13, 0, 0).timestamp() * 1000)
    now = int(datetime(2026, 8, 28, 13, 1, 20).timestamp() * 1000)

    result = scan_run_log(log, started, now)

    assert result["hard_error_count"] == 0
    assert result["pending_total"] == 1
    assert result["pending_resolved_total"] == 0
    assert result["pending_within_60_sec"] == []


def test_scan_run_log_keeps_only_unresolved_recent_pending(tmp_path):
    log = tmp_path / "bot.log"
    log.write_text(
        "2026-08-28 13:00:00 INFO new run\n"
        "2026-08-28 13:00:05 WARNING 订单状态等待对账: 订单 mm:ask:0 已显示 filled\n"
        "2026-08-28 13:00:06 INFO 成交 mm:ask:0 SELL 0.1 @ 100\n"
        "2026-08-28 13:00:10 WARNING 订单状态等待对账: 订单 mm:bid:0 已显示 filled\n",
        encoding="utf-8",
    )
    started = int(datetime(2026, 8, 28, 13, 0, 0).timestamp() * 1000)
    now = int(datetime(2026, 8, 28, 13, 0, 20).timestamp() * 1000)

    result = scan_run_log(log, started, now)

    assert result["pending_total"] == 2
    assert result["pending_resolved_total"] == 1
    assert len(result["pending_within_60_sec"]) == 1
    assert "mm:bid:0" in result["pending_within_60_sec"][0]


def test_scan_run_log_counts_open_order_reappearance_as_resolved(tmp_path):
    log = tmp_path / "bot.log"
    log.write_text(
        "2026-08-28 13:00:00 INFO new run\n"
        "2026-08-28 13:00:05 WARNING 订单状态等待对账: 订单 mm:bid:0 "
        "从开放委托消失\n"
        "2026-08-28 13:00:12 INFO 订单状态对账恢复: 订单 mm:bid:0 "
        "已重新出现在官方开放委托\n",
        encoding="utf-8",
    )
    started = int(datetime(2026, 8, 28, 13, 0, 0).timestamp() * 1000)
    now = int(datetime(2026, 8, 28, 13, 0, 20).timestamp() * 1000)

    result = scan_run_log(log, started, now)

    assert result["pending_total"] == 1
    assert result["pending_resolved_total"] == 1
    assert result["pending_within_60_sec"] == []


def test_scan_run_log_counts_generic_retired_open_recovery_as_resolved(tmp_path):
    log = tmp_path / "bot.log"
    log.write_text(
        "2026-08-28 13:00:00 INFO new run\n"
        "2026-08-28 13:00:05 WARNING 订单状态等待对账: "
        "1 笔已完成订单仍出现在开放委托快照；已暂停替换（5/60 秒）\n"
        "2026-08-28 13:00:12 INFO entropy.broker | "
        "已完成订单与开放委托快照的短暂矛盾已恢复\n",
        encoding="utf-8",
    )
    started = int(datetime(2026, 8, 28, 13, 0, 0).timestamp() * 1000)
    now = int(datetime(2026, 8, 28, 13, 0, 20).timestamp() * 1000)

    result = scan_run_log(log, started, now)

    assert result["pending_total"] == 1
    assert result["pending_resolved_total"] == 1
    assert result["pending_within_60_sec"] == []


def test_scan_run_log_reports_recent_rate_limits_and_timeouts_separately(tmp_path):
    log = tmp_path / "bot.log"
    log.write_text(
        "2026-08-28 13:00:00 INFO new run\n"
        "2026-08-28 13:00:05 WARNING 官方 /info 限流；5 秒后重试\n"
        "2026-08-28 13:00:10 WARNING request timed out\n",
        encoding="utf-8",
    )
    started = int(datetime(2026, 8, 28, 13, 0, 0).timestamp() * 1000)
    now = int(datetime(2026, 8, 28, 13, 1, 0).timestamp() * 1000)

    result = scan_run_log(log, started, now)

    assert result["hard_error_count"] == 0
    assert result["rate_limit_count"] == 1
    assert result["rate_limit_recent_5m"] == 1
    assert result["rate_limit_last_at_ms"] is not None
    assert result["timeout_count"] == 1
    assert result["timeout_recent_5m"] == 1
    assert result["timeout_last_at_ms"] is not None


def test_scan_run_log_ages_rate_limits_out_of_recent_window(tmp_path):
    log = tmp_path / "bot.log"
    log.write_text(
        "2026-08-28 13:00:00 INFO new run\n"
        "2026-08-28 13:00:05 WARNING HTTP 429 too many requests\n",
        encoding="utf-8",
    )
    started = int(datetime(2026, 8, 28, 13, 0, 0).timestamp() * 1000)
    now = int(datetime(2026, 8, 28, 13, 6, 0).timestamp() * 1000)

    result = scan_run_log(log, started, now)

    assert result["hard_error_count"] == 0
    assert result["rate_limit_count"] == 1
    assert result["rate_limit_recent_5m"] == 0


def test_scan_run_log_marks_error_recovered_after_same_process_heartbeat(tmp_path):
    log = tmp_path / "bot.log"
    log.write_text(
        "2026-08-28 13:00:00 INFO entropy.engine | 心跳 fair=100\n"
        "2026-08-28 13:00:05 ERROR entropy.engine | 主循环错误 (1/3): 429\n"
        "Traceback (most recent call last):\n"
        "2026-08-28 13:00:20 INFO entropy.engine | 成交 mm:bid:0 BUY 1 @ 100\n"
        "2026-08-28 13:00:21 INFO entropy.engine | 心跳 fair=100\n",
        encoding="utf-8",
    )
    started = int(datetime(2026, 8, 28, 13, 0, 0).timestamp() * 1000)
    now = int(datetime(2026, 8, 28, 13, 0, 30).timestamp() * 1000)

    result = scan_run_log(log, started, now)

    assert result["hard_error_count"] == 2
    assert result["hard_error_unresolved_count"] == 0
    assert result["hard_error_resolved_count"] == 2
    assert result["last_recovery_progress_at_ms"] is not None


def test_scan_run_log_keeps_error_unresolved_without_later_progress(tmp_path):
    log = tmp_path / "bot.log"
    log.write_text(
        "2026-08-28 13:00:00 INFO entropy.engine | 心跳 fair=100\n"
        "2026-08-28 13:00:05 ERROR entropy.engine | 主循环错误 (1/3): 429\n"
        "Traceback (most recent call last):\n",
        encoding="utf-8",
    )
    started = int(datetime(2026, 8, 28, 13, 0, 0).timestamp() * 1000)
    now = int(datetime(2026, 8, 28, 13, 0, 30).timestamp() * 1000)

    result = scan_run_log(log, started, now)

    assert result["hard_error_count"] == 2
    assert result["hard_error_unresolved_count"] == 2
    assert result["hard_error_resolved_count"] == 0


def test_process_alive_accepts_current_process():
    assert process_alive(os.getpid()) is True


def test_sanitize_log_line_redacts_secret_hex_address_and_url_auth():
    line = (
        "key=0x" + "a" * 64
        + " address=0x1234567890abcdef1234567890abcdef12345678"
        + " proxy=https://alice:secret@example.com/path"
    )

    sanitized = sanitize_log_line(line)

    assert "a" * 64 not in sanitized
    assert "1234567890abcdef1234567890abcdef12345678" not in sanitized
    assert "alice:secret" not in sanitized
    assert "<redacted-hex>" in sanitized
    assert "0x123456...5678" in sanitized
    assert "https://<redacted>@example.com/path" in sanitized
