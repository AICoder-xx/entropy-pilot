from __future__ import annotations

import math
from dataclasses import replace

from .config import Config


def equity_step_reference(cfg: Config, observed_equity_usd: float) -> float:
    """Return a stable, upward-only sizing candidate for deposited equity.

    The caller retains the high-water reference.  Flooring to a configured
    step prevents ordinary PnL noise from constantly resizing live quotes.
    """

    risk = cfg.risk
    if not risk.auto_scale_with_equity or observed_equity_usd <= risk.capital_usd:
        return risk.capital_usd
    capped = min(observed_equity_usd, risk.auto_scale_max_equity_usd)
    stepped = math.floor(capped / risk.auto_scale_step_usd) * risk.auto_scale_step_usd
    return max(risk.capital_usd, stepped)


def scale_config_for_equity(cfg: Config, equity_reference_usd: float) -> Config:
    """Build effective limits from an already stabilized equity reference."""

    risk = cfg.risk
    if not risk.auto_scale_with_equity:
        return cfg
    reference = min(
        max(equity_reference_usd, risk.capital_usd),
        risk.auto_scale_max_equity_usd,
    )
    order_floor = (
        cfg.market_maker.order_notional_usd
        if cfg.strategy == "market_maker"
        else cfg.grid.order_notional_usd
    )
    order_notional = min(
        max(order_floor, reference * risk.order_notional_pct_of_equity / 100.0),
        risk.order_notional_cap_usd,
    )
    max_open = max(
        risk.max_open_order_usd,
        reference * risk.max_open_order_pct_of_equity / 100.0,
        order_notional * 2.04,
    )
    effective_risk = replace(
        risk,
        capital_usd=reference,
        max_position_usd=max(
            risk.max_position_usd,
            reference * risk.max_position_pct_of_equity / 100.0,
        ),
        max_open_order_usd=max_open,
        min_free_collateral_usd=max(
            risk.min_free_collateral_usd,
            reference * risk.min_free_collateral_pct_of_equity / 100.0,
        ),
        max_session_drawdown_usd=max(
            risk.max_session_drawdown_usd,
            reference * risk.max_session_drawdown_pct_of_equity / 100.0,
        ),
        max_principal_drawdown_usd=max(
            risk.max_principal_drawdown_usd,
            reference * risk.max_principal_drawdown_pct_of_equity / 100.0,
        ),
    )
    if cfg.strategy == "market_maker":
        return replace(
            cfg,
            risk=effective_risk,
            market_maker=replace(
                cfg.market_maker, order_notional_usd=order_notional
            ),
        )
    return replace(
        cfg,
        risk=effective_risk,
        grid=replace(cfg.grid, order_notional_usd=order_notional),
    )


def effective_risk_profile(cfg: Config) -> dict[str, float | bool]:
    order_notional = (
        cfg.market_maker.order_notional_usd
        if cfg.strategy == "market_maker"
        else cfg.grid.order_notional_usd
    )
    return {
        "auto_scaled": cfg.risk.auto_scale_with_equity,
        "configured_capital_usd": cfg.risk.capital_usd,
        "order_notional_usd": order_notional,
        "max_position_usd": cfg.risk.max_position_usd,
        "max_open_order_usd": cfg.risk.max_open_order_usd,
        "min_free_collateral_usd": cfg.risk.min_free_collateral_usd,
        "max_drawdown_usd": cfg.risk.max_session_drawdown_usd,
        "max_principal_drawdown_usd": cfg.risk.max_principal_drawdown_usd,
        "max_rolling_drawdown_usd": cfg.risk.max_rolling_drawdown_usd,
        "rolling_drawdown_window_min": cfg.risk.rolling_drawdown_window_min,
        "daily_loss_budget_usd": cfg.risk.daily_loss_budget_usd,
    }
