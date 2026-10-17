from dataclasses import replace
from pathlib import Path

from entropy_bot.config import load_config
from entropy_bot.sizing import equity_step_reference, scale_config_for_equity


def _config():
    cfg = load_config(Path(__file__).parents[1] / "config.live.stage-a.example.json")
    return replace(
        cfg,
        risk=replace(
            cfg.risk,
            capital_usd=1000.0,
            max_position_usd=600.0,
            max_open_order_usd=410.0,
            min_free_collateral_usd=100.0,
            max_session_drawdown_usd=10.0,
            max_principal_drawdown_usd=20.0,
            auto_scale_with_equity=True,
            auto_scale_max_equity_usd=5000.0,
            auto_scale_step_usd=100.0,
            order_notional_pct_of_equity=25.0,
            order_notional_cap_usd=1250.0,
            max_position_pct_of_equity=60.0,
            max_open_order_pct_of_equity=52.0,
            min_free_collateral_pct_of_equity=10.0,
            max_session_drawdown_pct_of_equity=1.0,
            max_principal_drawdown_pct_of_equity=2.0,
        ),
        market_maker=replace(cfg.market_maker, order_notional_usd=200.0),
    )


def test_equity_sizing_scales_in_stable_steps_and_caps_at_5000():
    cfg = _config()
    assert equity_step_reference(cfg, 990.0) == 1000.0
    assert equity_step_reference(cfg, 4999.0) == 4900.0
    assert equity_step_reference(cfg, 9999.0) == 5000.0

    effective = scale_config_for_equity(cfg, 5000.0)
    assert effective.market_maker.order_notional_usd == 1250.0
    assert effective.risk.max_position_usd == 3000.0
    assert effective.risk.max_open_order_usd == 2600.0
    assert effective.risk.min_free_collateral_usd == 500.0
    assert effective.risk.max_session_drawdown_usd == 50.0
    assert effective.risk.max_principal_drawdown_usd == 100.0


def test_current_1000_equity_increases_order_without_expanding_position_cap():
    effective = scale_config_for_equity(_config(), 1000.0)
    assert effective.market_maker.order_notional_usd == 250.0
    assert effective.risk.max_position_usd == 600.0
    assert effective.risk.max_open_order_usd == 520.0
