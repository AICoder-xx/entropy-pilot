import json
import os
import time

import dashboard
from entropy_bot.models import AccountState, FeeModel, Fill, MarketMeta, MarketSnapshot, OrderBook
from entropy_bot.storage import Ledger, atomic_write_json, read_json


def test_atomic_json_retries_transient_windows_replace_lock(tmp_path, monkeypatch):
    target = tmp_path / "summary.json"
    real_replace = os.replace
    attempts = 0

    def flaky_replace(source, destination):
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise PermissionError("transient scanner lock")
        return real_replace(source, destination)

    monkeypatch.setattr("entropy_bot.storage.os.replace", flaky_replace)
    atomic_write_json(target, {"ok": True})

    assert attempts == 3
    assert json.loads(target.read_text(encoding="utf-8")) == {"ok": True}


def test_read_json_retries_transient_windows_open_lock(tmp_path, monkeypatch):
    target = tmp_path / "control.json"
    target.write_text('{"desired_state":"running"}', encoding="utf-8")
    real_open = type(target).open
    attempts = 0

    def flaky_open(self, *args, **kwargs):
        nonlocal attempts
        if self == target:
            attempts += 1
            if attempts < 3:
                raise PermissionError("transient scanner lock")
        return real_open(self, *args, **kwargs)

    monkeypatch.setattr(type(target), "open", flaky_open)
    assert read_json(target, None) == {"desired_state": "running"}
    assert attempts == 3


def market_snapshot():
    meta = MarketMeta(
        coin="io:SNDK",
        dex="io",
        symbol="SNDK",
        sz_decimals=4,
        max_leverage=10,
        only_isolated=True,
        growth_mode=True,
        deployer_fee_scale=1,
        is_delisted=False,
        dex_index=10,
        universe_index=2,
        asset_id=200002,
    )
    book = OrderBook()
    book.apply_hl(
        [[{"px": "100.04", "sz": "10"}], [{"px": "100.06", "sz": "10"}]]
    )
    return MarketSnapshot(meta, book, mark_px=100.05, oracle_px=100.05)


def fee_model():
    return FeeModel(
        user_maker_rate=0.00015,
        user_taker_rate=0.00045,
        active_hl_referral_discount=0,
        deployer_fee_scale=1,
        deployer_share=0.5,
        growth_mode=True,
        gross_maker_rate=0.00003,
        gross_taker_rate=0.00009,
        entropy_self_rebate_pct=160,
        entropy_referred_benefit_pct=0,
    )


def test_ledger_records_capital_wear_and_per_fill_markout(tmp_path, monkeypatch):
    ledger = Ledger(tmp_path, "paper", "market_maker", "io:SNDK", "session-a")
    fill = Fill(
        fill_id="fill-a",
        order_role="mm:bid:0",
        cloid="0x" + "1" * 32,
        oid=7,
        is_buy=True,
        price=100.0,
        qty=1.0,
        fee_usd=0.003,
        liquidity="maker",
        time_ms=int(time.time() * 1000) - 2_000,
        realized_pnl_delta=0.0,
        projected_self_rebate=0.0024,
    )
    ledger.record_fill(
        fill,
        "paper",
        "market_maker",
        "io:SNDK",
        mid_at_record=100.01,
        account_equity_after=999.0,
    )
    ledger.record_markout(fill, 1, 100.05)
    ledger.record_markout(fill, 30, 100.05)
    account = AccountState(
        equity_actual=999.0,
        realized_pnl=-0.997,
        fees_paid=0.003,
        projected_self_rebate=0.0024,
        volume_usd=100.0,
    )
    ledger.write_summary(
        status="running",
        reason=None,
        account=account,
        market=market_snapshot(),
        fees=fee_model(),
        open_orders=2,
        session_start_equity=1000.0,
        session_started_at_ms=1,
        configured_capital_usd=1000.0,
    )

    summary = json.loads(ledger.summary_path.read_text(encoding="utf-8"))
    assert summary["actual_wear_usd"] == 1.0
    assert summary["principal_wear_usd"] == 1.0
    assert summary["account_session_wear_usd"] == 1.0
    assert summary["actual_wear_bps_of_start_capital"] == 10.0
    assert summary["actual_wear_bps_per_traded_notional"] == 100.0
    assert summary["volume_usd"] == 100.0

    monkeypatch.setattr(dashboard, "STATE_DIR", tmp_path)
    snapshot = dashboard.build_snapshot(ledger.slug)
    row = snapshot["fills"][0]
    assert row["notional_usd"] == 100.0
    assert abs(row["projected_net_fee_usd"] - 0.0006) < 1e-12
    assert abs(row["markouts"]["1"]["markout_bps"] - 5.0) < 1e-9
    assert row["markouts"]["1"]["order_role"] == "mm:bid:0"
    assert row["markouts"]["1"]["side"] == "BUY"
    assert row["markouts"]["1"]["level"] == 0
    assert row["markouts"]["1"]["liquidity"] == "maker"
    assert row["markouts"]["1"]["fill_px"] == 100.0
    assert row["latest_execution_wear_usd"] < 0
    aggregates = snapshot["summary"]["dashboard_aggregates"]
    assert aggregates["markout_30_samples"] == 1
    assert aggregates["markout_30_observed_notional_usd"] == 100.0
    assert abs(aggregates["markout_30_weighted_bps"] - 5.0) < 1e-9
    assert abs(aggregates["markout_30_projected_execution_wear_bps"] + 4.94) < 1e-9
