import json
from audit_local_data import audit


def write_rows(tmp_path, rows):
    state = tmp_path / "state"
    state.mkdir(exist_ok=True)
    (state / "live_market_maker_io_TEST_fills.jsonl").write_text("\n".join(json.dumps(r) for r in rows))


def fill(**extra):
    return dict(mode="live", coin="io:TEST", fill_id="a", oid=1, time_ms=1000000,
                price=10, qty=2, is_buy=True, fee_usd=0.1, realized_pnl_delta=1,
                notional_usd=20, **extra)


def test_conflicting_identity_makes_pnl_unknown(tmp_path):
    row = fill()
    write_rows(tmp_path, [row, {**row, "realized_pnl_delta": 9}])
    result = audit(tmp_path)
    assert result["diagnostics"]["conflicting_fill_identities"] == 1
    assert result["total"]["realized_less_fees_usd"] is None
    assert not result["total"]["complete"]


def test_missing_notional_is_unknown_not_zero(tmp_path):
    row = fill()
    row.pop("notional_usd")
    write_rows(tmp_path, [row])
    result = audit(tmp_path)["total"]
    assert result["volume_usd"] is None
    assert result["missing_notional_rows"] == 1
    assert result["booked_cost_per_million_usd"] is None


def test_identical_rows_dedupe_and_paper_is_excluded(tmp_path):
    row = fill()
    write_rows(tmp_path, [row, row, {**row, "mode": "paper"}])
    result = audit(tmp_path)
    assert result["total"]["fills"] == 1
    assert result["total"]["realized_less_fees_usd"] == 0.9
    assert result["paper_fills_excluded"] == 1
