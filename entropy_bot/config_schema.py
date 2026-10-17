from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, get_type_hints


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class MarketConfig:
    dex: str
    symbol: str
    api_url: str
    ws_url: str
    max_book_age_sec: float
    context_refresh_sec: float
    require_growth_mode: bool
    target_leverage: int | None = None


@dataclass(frozen=True)
class FeeConfig:
    entropy_self_rebate_pct: float
    entropy_referred_user_benefit_pct: float
    fail_closed_if_user_fees_unavailable: bool


@dataclass(frozen=True)
class RiskConfig:
    capital_usd: float
    max_position_usd: float
    max_open_order_usd: float
    min_order_notional_usd: float
    min_free_collateral_usd: float
    max_session_drawdown_usd: float
    max_principal_drawdown_usd: float
    max_oracle_deviation_bps: float
    max_book_spread_bps: float
    max_consecutive_errors: int
    flatten_on_halt: bool
    emergency_slippage_bps: float
    auto_scale_with_equity: bool
    auto_scale_max_equity_usd: float
    auto_scale_step_usd: float
    order_notional_pct_of_equity: float
    order_notional_cap_usd: float
    max_position_pct_of_equity: float
    max_open_order_pct_of_equity: float
    min_free_collateral_pct_of_equity: float
    max_session_drawdown_pct_of_equity: float
    max_principal_drawdown_pct_of_equity: float
    # Loss-velocity guards (0 disables).  Slow accumulated wear is an
    # operating cost handled by the daily budget; a fast drawdown inside the
    # rolling window is an incident.  Both halt (and flatten) like the other
    # lines, but the daily-budget reason explicitly allows a next-day restart.
    max_rolling_drawdown_usd: float = 0.0
    rolling_drawdown_window_min: float = 30.0
    daily_loss_budget_usd: float = 0.0


@dataclass(frozen=True)
class MarketMakerConfig:
    levels: int
    order_notional_usd: float
    half_spread_bps: float
    level_spacing_bps: float
    inventory_skew_bps_at_limit: float
    rebalance_at_position_ratio: float
    min_edge_after_projected_rebate_bps: float
    refresh_bps: float
    refresh_sec: float
    adaptive_spread_enabled: bool
    adaptive_spread_min_samples: int
    adaptive_spread_window_samples: int
    adaptive_spread_adjust_every_samples: int
    adaptive_spread_wear_trigger_bps: float
    adaptive_spread_step_bps: float
    adaptive_spread_max_extra_bps: float
    # 60 s realized price range (bps) beyond which quoting turns
    # exposure-reducing-only until the range halves.  0 disables the guard.
    volatility_guard_range_bps: float = 20.0
    # After a fill, do not re-quote the exposure-increasing side for this
    # many seconds. A cooldown avoids immediately repeating entry orders
    # into the same sweep. Zero disables the cooldown.
    same_side_fill_cooldown_sec: float = 10.0
    # Crisis (dislocation) quoting: when the book is too wide/empty to make
    # markets normally, optionally park small far-touch orders around a LIVE
    # oracle anchor and harvest the mean reversion after panic flow sweeps a
    # thin book.  notional 0 disables (default); the engine additionally
    # requires the oracle to have CHANGED recently before quoting.
    crisis_quote_notional_usd: float = 0.0
    crisis_quote_distance_bps: float = 80.0
    # Avellaneda-Stoikov style volatility scaling (0 disables either term).
    # half_spread_bps then acts as a FLOOR: effective half-spread is
    # max(floor, vol_spread_mult * sigma_60s) + adaptive extra, and the
    # inventory skew unit is max(inventory_skew_bps_at_limit,
    # vol_skew_mult * sigma_60s) capped at 3x the configured skew.
    vol_spread_mult: float = 0.6
    vol_skew_mult: float = 0.8
    # ``order_notional_usd`` is the TOTAL budget for one side of the ladder.
    # Additional levels split that budget geometrically instead of multiplying
    # exposure/action cost by ``levels``.  0.35 gives a two-level split of
    # roughly 74% / 26%, keeping the outer quote deliberately small.
    level_notional_decay: float = 0.35
    # Thin HIP-3 books can contain our own resting size.  Blending the raw
    # top-level microprice into the quote centre is therefore opt-in.
    microprice_weight: float = 0.0
    # Moving away from flow protects inventory and may happen at refresh_bps.
    # Chasing the market sacrifices queue position, so it needs both a wider
    # threshold and a minimum resting age.
    aggressive_requote_multiplier: float = 3.0
    aggressive_requote_min_age_sec: float = 3.0
    # Ignore tiny fixed-dollar quantity changes caused by one price tick.
    qty_refresh_ratio: float = 0.05
    # Adaptive samples are regime-scoped and time-bounded.  When a wide quote
    # receives no fills, slowly release the extra spread so it cannot self-lock.
    adaptive_spread_idle_decay_sec: float = 1800.0
    adaptive_spread_sample_max_age_sec: float = 21600.0


@dataclass(frozen=True)
class GridConfig:
    anchor_price: float | None
    levels_each_side: int
    spacing_bps: float
    order_notional_usd: float
    min_profit_after_projected_rebate_bps: float
    breakout_buffer_bps: float
    recenter_when_flat_bps: float
    level_rearm_bps: float
    inventory_direction: str = "long"


@dataclass(frozen=True)
class RuntimeConfig:
    loop_interval_sec: float
    account_refresh_sec: float
    fill_refresh_sec: float
    heartbeat_sec: float
    summary_refresh_sec: float
    cancel_strategy_orders_on_start: bool
    cancel_strategy_orders_on_exit: bool
    pause_during_us_regular_session: bool
    us_regular_session_pre_open_buffer_min: int
    us_regular_session_post_close_buffer_min: int
    state_dir: Path
    log_dir: Path
    # Websocket push-state (official user channels) as the fast data path;
    # REST stays authoritative on the sweep cadence below.
    use_ws_user_feeds: bool = True
    rest_sweep_sec: float = 60.0
    use_batch_modify: bool = True
    # Address-based L1 action budget management.
    budget_poll_sec: float = 60.0
    budget_soft_remaining: int = 2500
    budget_hard_remaining: int = 800
    auto_reserve_actions: bool = False
    reserve_actions_chunk: int = 10_000
    reserve_actions_daily_max_usd: float = 10.0


@dataclass(frozen=True)
class PaperConfig:
    initial_equity_usd: float
    touch_fill: bool


@dataclass(frozen=True)
class LiveConfig:
    private_key_env: str
    account_address_env: str
    credentials_file: Path
    wallet_id: str | None
    # Explicit opt-in; existing configurations must not discover an unrelated
    # .env file in this directory or a parent directory.
    env_file: Path | None = None


@dataclass(frozen=True)
class Config:
    mode: str
    strategy: str
    market: MarketConfig
    fees: FeeConfig
    risk: RiskConfig
    market_maker: MarketMakerConfig
    grid: GridConfig
    runtime: RuntimeConfig
    paper: PaperConfig
    live: LiveConfig
    source_path: Path


def _section(raw: dict, name: str, allowed: set[str]) -> dict:
    value = raw.get(name)
    if not isinstance(value, dict):
        raise ConfigError(f"配置段 {name!r} 缺失或不是对象")
    unknown = set(value) - allowed
    if unknown:
        raise ConfigError(f"配置段 {name!r} 含未知字段: {sorted(unknown)}")
    section_types = {
        "market": MarketConfig, "fees": FeeConfig, "risk": RiskConfig,
        "market_maker": MarketMakerConfig, "grid": GridConfig,
        "runtime": RuntimeConfig, "paper": PaperConfig, "live": LiveConfig,
    }
    # Validate the JSON types before optional int()/float() conversions.
    # bool is an int subclass, and float(NaN) defeats ordinary limit checks.
    for key, typ in get_type_hints(section_types[name]).items():
        if key not in value or typ not in (int, float):
            continue
        number = value[key]
        valid_type = isinstance(number, int) if typ is int else isinstance(number, (int, float))
        if isinstance(number, bool) or not valid_type:
            raise ConfigError(f"配置字段 {name}.{key} 必须是{'整数' if typ is int else '数值'}")
        try:
            finite = math.isfinite(number)
        except OverflowError:
            finite = False
        if not finite:
            raise ConfigError(f"配置字段 {name}.{key} 必须是有限数值")
    return value


def _need(d: dict, key: str, typ: type | tuple[type, ...]) -> Any:
    if key not in d:
        raise ConfigError(f"缺少配置字段: {key}")
    value = d[key]
    if typ in (int, float, (int, float)) and isinstance(value, bool):
        raise ConfigError(f"配置字段 {key} 类型错误")
    if not isinstance(value, typ):
        raise ConfigError(f"配置字段 {key} 类型错误: {type(value).__name__}")
    return value


def _positive(name: str, value: float, allow_zero: bool = False) -> None:
    if not math.isfinite(value) or value < 0 or (value == 0 and not allow_zero):
        op = ">= 0" if allow_zero else "> 0"
        raise ConfigError(f"{name} 必须 {op}")
