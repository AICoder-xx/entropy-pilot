from __future__ import annotations

import math

from .models import FeeModel, MarketMeta


def build_fee_model(
    meta: MarketMeta,
    user_fees: dict | None,
    *,
    self_rebate_pct: float,
    referred_benefit_pct: float,
) -> FeeModel:
    """Apply Hyperliquid's published HIP-3 fee formula.

    userAddRate/userCrossRate already include the user's fee tier and staking
    discount. HIP-3 scale, Growth Mode and active HL referral discount are
    applied here. Entropy's early-bird benefits are projected separately and
    are never counted as realized cash.
    """
    def number(name: str, default: float) -> float:
        if user_fees is None:
            return default
        raw = user_fees.get(name)
        # A returned but incomplete account response must not silently become
        # the generic tier; numeric zero is a valid published fee rate.
        if name == "activeReferralDiscount" and raw is None:
            return default
        if raw in (None, "") or isinstance(raw, bool):
            raise ValueError(f"userFees 的 {name} 缺失或无效")
        value = float(raw)
        if not math.isfinite(value):
            raise ValueError(f"userFees 的 {name} 必须是有限数值")
        return value

    maker = number("userAddRate", 0.00015)
    taker = number("userCrossRate", 0.00045)
    hl_ref = number("activeReferralDiscount", 0.0)
    if taker < 0 or not 0 <= hl_ref <= 1:
        raise ValueError("userFees 的 taker 费率或推荐折扣超出有效范围")
    scale = meta.deployer_fee_scale
    if scale < 1:
        hip3_scale = 1.0 + scale
        deployer_share = scale / (1.0 + scale) if scale > 0 else 0.0
    else:
        hip3_scale = scale * 2.0
        deployer_share = 0.5
    growth = 0.1 if meta.growth_mode else 1.0

    # Published formula: positive maker fees are HIP-3 scaled and discounted;
    # negative maker rebates are only Growth-Mode scaled.
    gross_maker = maker * growth
    if gross_maker > 0:
        gross_maker *= hip3_scale * (1.0 - hl_ref)
    gross_taker = taker * hip3_scale * growth * (1.0 - hl_ref)

    return FeeModel(
        user_maker_rate=maker,
        user_taker_rate=taker,
        active_hl_referral_discount=hl_ref,
        deployer_fee_scale=scale,
        deployer_share=deployer_share,
        growth_mode=meta.growth_mode,
        gross_maker_rate=gross_maker,
        gross_taker_rate=gross_taker,
        entropy_self_rebate_pct=self_rebate_pct,
        entropy_referred_benefit_pct=referred_benefit_pct,
    )

