from entropy_bot.fees import build_fee_model
from entropy_bot.models import MarketMeta


def meta(growth=True, scale=1.0):
    return MarketMeta(
        coin="io:SNDK",
        dex="io",
        symbol="SNDK",
        sz_decimals=4,
        max_leverage=10,
        only_isolated=True,
        growth_mode=growth,
        deployer_fee_scale=scale,
        is_delisted=False,
        dex_index=1,
        universe_index=2,
        asset_id=110002,
    )


def test_tier3_growth_mode_base_fee_math():
    f = build_fee_model(
        meta(),
        {"userAddRate": "0.00015", "userCrossRate": "0.00045"},
        self_rebate_pct=160,
        referred_benefit_pct=0,
    )
    assert abs(f.gross_maker_rate * 10_000 - 0.30) < 1e-12
    assert abs(f.gross_taker_rate * 10_000 - 0.90) < 1e-12
    assert abs(f.projected_net_maker_rate * 10_000 - 0.06) < 1e-12
    assert abs(f.projected_net_taker_rate * 10_000 - 0.18) < 1e-12
    own, benefit = f.projected_entropy_rebate(30.0)
    assert own == 24.0 and benefit == 0.0


def test_referred_benefit_is_separate_and_can_make_subsidy():
    f = build_fee_model(
        meta(),
        None,
        self_rebate_pct=160,
        referred_benefit_pct=75,
    )
    assert f.projected_net_maker_rate < 0


def test_negative_maker_rebate_is_not_hip3_doubled():
    f = build_fee_model(
        meta(),
        {"userAddRate": "-0.00001", "userCrossRate": "0.00045"},
        self_rebate_pct=160,
        referred_benefit_pct=0,
    )
    assert abs(f.gross_maker_rate - (-0.000001)) < 1e-15

