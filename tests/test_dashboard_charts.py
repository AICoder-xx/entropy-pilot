from dashboard_charts import build_charts


def chart(rows):
    return build_charts(rows, profile_id="test", session_id="s", wallet_id="one", coin="io:X", mode="live")


def test_charts_accumulate_real_fills_sort_dedupe_and_preserve_unknown():
    a = {"fill_id": "a", "time_ms": 1000, "notional_usd": 10, "fee_usd": 0.1, "realized_pnl_delta": 1, "is_buy": True}
    b = {**a, "fill_id": "b", "time_ms": 61000, "fee_usd": None, "is_buy": False}
    result = chart([b, a, a])
    assert result["scope"]["fill_count"] == 2
    assert result["series"][0]["cumulative_realized_less_fees_usd"] == 0.9
    assert result["series"][-1]["cumulative_fee_usd"] is None
    assert result["series"][-1]["cumulative_realized_less_fees_usd"] is None
    assert result["series"][-1]["cumulative_volume_usd"] == 20
    assert not result["scope"]["complete"]
    assert result["sides"]["buy"]["count"] == result["sides"]["sell"]["count"] == 1


def test_empty_chart_is_empty_not_fabricated_zero_curve():
    assert chart([])["series"] == []
    assert not chart([])["scope"]["complete"]
