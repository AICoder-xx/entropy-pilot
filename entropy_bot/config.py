from __future__ import annotations

import json
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


ROOT_KEYS = {
    "mode", "strategy", "market", "fees", "risk", "market_maker", "grid",
    "runtime", "paper", "live", "_说明"
}


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


def load_config(path: str | Path) -> Config:
    source = Path(path).resolve()
    with source.open("r", encoding="utf-8") as f:
        raw = json.load(f)
    if not isinstance(raw, dict):
        raise ConfigError("配置文件顶层必须是对象")
    # Underscore-prefixed top-level keys are documentation/audit metadata by
    # project convention (_说明, _tuning_applied, ...), never configuration.
    unknown = {key for key in set(raw) - ROOT_KEYS if not key.startswith("_")}
    if unknown:
        raise ConfigError(f"配置文件含未知字段: {sorted(unknown)}")

    mode = _need(raw, "mode", str).lower()
    strategy = _need(raw, "strategy", str).lower()
    if mode not in {"paper", "live"}:
        raise ConfigError("mode 只能是 paper 或 live")
    if strategy not in {"market_maker", "grid"}:
        raise ConfigError("strategy 只能是 market_maker 或 grid")

    m = _section(raw, "market", {
        "dex", "symbol", "api_url", "ws_url", "max_book_age_sec",
        "context_refresh_sec", "require_growth_mode", "target_leverage"
    })
    target_leverage = m.get("target_leverage")
    if target_leverage is not None and (
        not isinstance(target_leverage, int)
        or isinstance(target_leverage, bool)
        or target_leverage < 1
    ):
        raise ConfigError("market.target_leverage 必须为 null 或正整数")
    market = MarketConfig(
        dex=_need(m, "dex", str),
        symbol=_need(m, "symbol", str).upper(),
        api_url=_need(m, "api_url", str).rstrip("/"),
        ws_url=_need(m, "ws_url", str),
        max_book_age_sec=float(_need(m, "max_book_age_sec", (int, float))),
        context_refresh_sec=float(_need(m, "context_refresh_sec", (int, float))),
        require_growth_mode=_need(m, "require_growth_mode", bool),
        target_leverage=target_leverage,
    )
    if market.dex != "io":
        raise ConfigError("本版本只允许 Entropy dex: io")
    _positive("market.max_book_age_sec", market.max_book_age_sec)
    _positive("market.context_refresh_sec", market.context_refresh_sec)

    fcfg = _section(raw, "fees", {
        "entropy_self_rebate_pct", "entropy_referred_user_benefit_pct",
        "fail_closed_if_user_fees_unavailable"
    })
    fees = FeeConfig(
        entropy_self_rebate_pct=float(_need(fcfg, "entropy_self_rebate_pct", (int, float))),
        entropy_referred_user_benefit_pct=float(
            _need(fcfg, "entropy_referred_user_benefit_pct", (int, float))
        ),
        fail_closed_if_user_fees_unavailable=_need(
            fcfg, "fail_closed_if_user_fees_unavailable", bool
        ),
    )
    for name, value in (
        ("entropy_self_rebate_pct", fees.entropy_self_rebate_pct),
        ("entropy_referred_user_benefit_pct", fees.entropy_referred_user_benefit_pct),
    ):
        if not 0 <= value <= 300:
            raise ConfigError(f"{name} 必须在 0..300")

    r = _section(raw, "risk", {
        "capital_usd", "max_position_usd", "max_open_order_usd",
        "min_order_notional_usd", "min_free_collateral_usd",
        "max_session_drawdown_usd", "max_principal_drawdown_usd",
        "max_oracle_deviation_bps",
        "max_book_spread_bps", "max_consecutive_errors", "flatten_on_halt",
        "emergency_slippage_bps", "max_rolling_drawdown_usd",
        "rolling_drawdown_window_min", "daily_loss_budget_usd",
        "auto_scale_with_equity",
        "auto_scale_max_equity_usd", "auto_scale_step_usd",
        "order_notional_pct_of_equity", "order_notional_cap_usd",
        "max_position_pct_of_equity", "max_open_order_pct_of_equity",
        "min_free_collateral_pct_of_equity",
        "max_session_drawdown_pct_of_equity",
        "max_principal_drawdown_pct_of_equity"
    })
    max_session_drawdown_usd = float(
        _need(r, "max_session_drawdown_usd", (int, float))
    )
    auto_scale_with_equity = r.get("auto_scale_with_equity", False)
    if not isinstance(auto_scale_with_equity, bool):
        raise ConfigError("auto_scale_with_equity 必须是布尔值")
    risk = RiskConfig(
        capital_usd=float(_need(r, "capital_usd", (int, float))),
        max_position_usd=float(_need(r, "max_position_usd", (int, float))),
        max_open_order_usd=float(_need(r, "max_open_order_usd", (int, float))),
        min_order_notional_usd=float(_need(r, "min_order_notional_usd", (int, float))),
        min_free_collateral_usd=float(_need(r, "min_free_collateral_usd", (int, float))),
        max_session_drawdown_usd=max_session_drawdown_usd,
        max_principal_drawdown_usd=float(
            r.get("max_principal_drawdown_usd", max_session_drawdown_usd * 2)
        ),
        max_oracle_deviation_bps=float(
            _need(r, "max_oracle_deviation_bps", (int, float))
        ),
        max_book_spread_bps=float(_need(r, "max_book_spread_bps", (int, float))),
        max_consecutive_errors=int(_need(r, "max_consecutive_errors", int)),
        flatten_on_halt=_need(r, "flatten_on_halt", bool),
        emergency_slippage_bps=float(_need(r, "emergency_slippage_bps", (int, float))),
        max_rolling_drawdown_usd=float(r.get("max_rolling_drawdown_usd", 0.0)),
        rolling_drawdown_window_min=float(
            r.get("rolling_drawdown_window_min", 30.0)
        ),
        daily_loss_budget_usd=float(r.get("daily_loss_budget_usd", 0.0)),
        auto_scale_with_equity=auto_scale_with_equity,
        auto_scale_max_equity_usd=float(
            r.get("auto_scale_max_equity_usd", r["capital_usd"])
        ),
        auto_scale_step_usd=float(r.get("auto_scale_step_usd", 100.0)),
        order_notional_pct_of_equity=float(
            r.get("order_notional_pct_of_equity", 25.0)
        ),
        order_notional_cap_usd=float(
            r.get("order_notional_cap_usd", r["capital_usd"])
        ),
        max_position_pct_of_equity=float(
            r.get("max_position_pct_of_equity", 60.0)
        ),
        max_open_order_pct_of_equity=float(
            r.get("max_open_order_pct_of_equity", 52.0)
        ),
        min_free_collateral_pct_of_equity=float(
            r.get("min_free_collateral_pct_of_equity", 10.0)
        ),
        max_session_drawdown_pct_of_equity=float(
            r.get("max_session_drawdown_pct_of_equity", 1.0)
        ),
        max_principal_drawdown_pct_of_equity=float(
            r.get("max_principal_drawdown_pct_of_equity", 2.0)
        ),
    )
    for name, value in (
        ("capital_usd", risk.capital_usd),
        ("max_position_usd", risk.max_position_usd),
        ("max_open_order_usd", risk.max_open_order_usd),
        ("min_order_notional_usd", risk.min_order_notional_usd),
        ("max_session_drawdown_usd", risk.max_session_drawdown_usd),
        ("max_principal_drawdown_usd", risk.max_principal_drawdown_usd),
    ):
        _positive(name, value)
    if risk.max_position_usd > risk.capital_usd:
        raise ConfigError("max_position_usd 不得高于 capital_usd（本版本不以杠杆放大风险限额）")
    if risk.max_principal_drawdown_usd >= risk.capital_usd:
        raise ConfigError("max_principal_drawdown_usd 必须低于 capital_usd")
    for name in ("min_free_collateral_usd", "max_oracle_deviation_bps", "max_book_spread_bps", "emergency_slippage_bps"):
        _positive(f"risk.{name}", getattr(risk, name), allow_zero=True)
    if risk.max_consecutive_errors < 1:
        raise ConfigError("risk.max_consecutive_errors 必须是正整数")
    for name, value in (
        ("max_rolling_drawdown_usd", risk.max_rolling_drawdown_usd),
        ("daily_loss_budget_usd", risk.daily_loss_budget_usd),
    ):
        _positive(name, value, allow_zero=True)
    if not 1.0 <= risk.rolling_drawdown_window_min <= 240.0:
        raise ConfigError("rolling_drawdown_window_min 必须在 1..240 分钟")
    if risk.auto_scale_max_equity_usd < risk.capital_usd:
        raise ConfigError("auto_scale_max_equity_usd 不得低于 capital_usd")
    _positive("auto_scale_step_usd", risk.auto_scale_step_usd)
    _positive("order_notional_cap_usd", risk.order_notional_cap_usd)
    for name, value in (
        ("order_notional_pct_of_equity", risk.order_notional_pct_of_equity),
        ("max_position_pct_of_equity", risk.max_position_pct_of_equity),
        ("max_open_order_pct_of_equity", risk.max_open_order_pct_of_equity),
        ("min_free_collateral_pct_of_equity", risk.min_free_collateral_pct_of_equity),
        ("max_session_drawdown_pct_of_equity", risk.max_session_drawdown_pct_of_equity),
        ("max_principal_drawdown_pct_of_equity", risk.max_principal_drawdown_pct_of_equity),
    ):
        if not 0 < value <= 300:
            raise ConfigError(f"{name} 必须在 (0, 300]")
    if risk.max_open_order_pct_of_equity < 2 * risk.order_notional_pct_of_equity:
        raise ConfigError(
            "max_open_order_pct_of_equity 必须至少是 order_notional_pct_of_equity 的两倍"
        )

    mm = _section(raw, "market_maker", {
        "levels", "order_notional_usd", "half_spread_bps", "level_spacing_bps",
        "inventory_skew_bps_at_limit", "rebalance_at_position_ratio",
        "min_edge_after_projected_rebate_bps", "refresh_bps", "refresh_sec",
        "adaptive_spread_enabled", "adaptive_spread_min_samples",
        "adaptive_spread_window_samples", "adaptive_spread_adjust_every_samples",
        "adaptive_spread_wear_trigger_bps", "adaptive_spread_step_bps",
        "adaptive_spread_max_extra_bps", "volatility_guard_range_bps",
        "same_side_fill_cooldown_sec", "crisis_quote_notional_usd",
        "crisis_quote_distance_bps", "vol_spread_mult", "vol_skew_mult",
        "level_notional_decay", "microprice_weight",
        "aggressive_requote_multiplier", "aggressive_requote_min_age_sec",
        "qty_refresh_ratio", "adaptive_spread_idle_decay_sec",
        "adaptive_spread_sample_max_age_sec"
    })
    adaptive_enabled = mm.get("adaptive_spread_enabled", False)
    if not isinstance(adaptive_enabled, bool):
        raise ConfigError("market_maker.adaptive_spread_enabled 必须为布尔值")
    market_maker = MarketMakerConfig(
        levels=int(_need(mm, "levels", int)),
        order_notional_usd=float(_need(mm, "order_notional_usd", (int, float))),
        half_spread_bps=float(_need(mm, "half_spread_bps", (int, float))),
        level_spacing_bps=float(_need(mm, "level_spacing_bps", (int, float))),
        inventory_skew_bps_at_limit=float(
            _need(mm, "inventory_skew_bps_at_limit", (int, float))
        ),
        rebalance_at_position_ratio=float(
            _need(mm, "rebalance_at_position_ratio", (int, float))
        ),
        min_edge_after_projected_rebate_bps=float(
            _need(mm, "min_edge_after_projected_rebate_bps", (int, float))
        ),
        refresh_bps=float(_need(mm, "refresh_bps", (int, float))),
        refresh_sec=float(_need(mm, "refresh_sec", (int, float))),
        adaptive_spread_enabled=adaptive_enabled,
        adaptive_spread_min_samples=int(mm.get("adaptive_spread_min_samples", 100)),
        adaptive_spread_window_samples=int(mm.get("adaptive_spread_window_samples", 200)),
        adaptive_spread_adjust_every_samples=int(
            mm.get("adaptive_spread_adjust_every_samples", 100)
        ),
        adaptive_spread_wear_trigger_bps=float(
            mm.get("adaptive_spread_wear_trigger_bps", 1.0)
        ),
        adaptive_spread_step_bps=float(mm.get("adaptive_spread_step_bps", 0.5)),
        adaptive_spread_max_extra_bps=float(
            mm.get("adaptive_spread_max_extra_bps", 2.0)
        ),
        volatility_guard_range_bps=float(
            mm.get("volatility_guard_range_bps", 20.0)
        ),
        same_side_fill_cooldown_sec=float(
            mm.get("same_side_fill_cooldown_sec", 10.0)
        ),
        crisis_quote_notional_usd=float(
            mm.get("crisis_quote_notional_usd", 0.0)
        ),
        crisis_quote_distance_bps=float(
            mm.get("crisis_quote_distance_bps", 80.0)
        ),
        vol_spread_mult=float(mm.get("vol_spread_mult", 0.6)),
        vol_skew_mult=float(mm.get("vol_skew_mult", 0.8)),
        level_notional_decay=float(mm.get("level_notional_decay", 0.35)),
        microprice_weight=float(mm.get("microprice_weight", 0.0)),
        aggressive_requote_multiplier=float(
            mm.get("aggressive_requote_multiplier", 3.0)
        ),
        aggressive_requote_min_age_sec=float(
            mm.get("aggressive_requote_min_age_sec", 3.0)
        ),
        qty_refresh_ratio=float(mm.get("qty_refresh_ratio", 0.05)),
        adaptive_spread_idle_decay_sec=float(
            mm.get("adaptive_spread_idle_decay_sec", 1800.0)
        ),
        adaptive_spread_sample_max_age_sec=float(
            mm.get("adaptive_spread_sample_max_age_sec", 21600.0)
        ),
    )
    if not 1 <= market_maker.levels <= 10:
        raise ConfigError("market_maker.levels 必须在 1..10")
    for name in ("order_notional_usd", "refresh_sec"):
        _positive(f"market_maker.{name}", getattr(market_maker, name))
    for name in ("half_spread_bps", "level_spacing_bps", "inventory_skew_bps_at_limit", "min_edge_after_projected_rebate_bps", "refresh_bps"):
        _positive(f"market_maker.{name}", getattr(market_maker, name), allow_zero=True)
    if not 0 < market_maker.rebalance_at_position_ratio <= 1:
        raise ConfigError("market_maker.rebalance_at_position_ratio 必须在 (0, 1]")
    if market_maker.adaptive_spread_min_samples < 20:
        raise ConfigError("market_maker.adaptive_spread_min_samples 不得低于 20")
    if (
        market_maker.adaptive_spread_window_samples
        < market_maker.adaptive_spread_min_samples
    ):
        raise ConfigError(
            "market_maker.adaptive_spread_window_samples 不得低于 min_samples"
        )
    if market_maker.adaptive_spread_adjust_every_samples < 20:
        raise ConfigError(
            "market_maker.adaptive_spread_adjust_every_samples 不得低于 20"
        )
    _positive(
        "market_maker.adaptive_spread_wear_trigger_bps",
        market_maker.adaptive_spread_wear_trigger_bps,
    )
    _positive(
        "market_maker.adaptive_spread_step_bps",
        market_maker.adaptive_spread_step_bps,
    )
    _positive(
        "market_maker.adaptive_spread_max_extra_bps",
        market_maker.adaptive_spread_max_extra_bps,
    )
    if not 0 <= market_maker.volatility_guard_range_bps <= 500:
        raise ConfigError(
            "market_maker.volatility_guard_range_bps 必须在 0..500（0=禁用）"
        )
    if not 0 <= market_maker.same_side_fill_cooldown_sec <= 60:
        raise ConfigError(
            "market_maker.same_side_fill_cooldown_sec 必须在 0..60（0=禁用）"
        )
    _positive(
        "market_maker.crisis_quote_notional_usd",
        market_maker.crisis_quote_notional_usd,
        allow_zero=True,
    )
    if not 10 <= market_maker.crisis_quote_distance_bps <= 500:
        raise ConfigError(
            "market_maker.crisis_quote_distance_bps 必须在 10..500"
        )
    for name, value in (
        ("vol_spread_mult", market_maker.vol_spread_mult),
        ("vol_skew_mult", market_maker.vol_skew_mult),
    ):
        if not 0 <= value <= 3:
            raise ConfigError(f"market_maker.{name} 必须在 0..3（0=禁用）")
    if not 0 < market_maker.level_notional_decay <= 1:
        raise ConfigError("market_maker.level_notional_decay 必须在 (0, 1]")
    if not 0 <= market_maker.microprice_weight <= 1:
        raise ConfigError("market_maker.microprice_weight 必须在 0..1")
    if not 1 <= market_maker.aggressive_requote_multiplier <= 10:
        raise ConfigError("market_maker.aggressive_requote_multiplier 必须在 1..10")
    if not 0 <= market_maker.aggressive_requote_min_age_sec <= 60:
        raise ConfigError("market_maker.aggressive_requote_min_age_sec 必须在 0..60")
    if not 0.001 <= market_maker.qty_refresh_ratio <= 1:
        raise ConfigError("market_maker.qty_refresh_ratio 必须在 0.001..1")
    if not 0 <= market_maker.adaptive_spread_idle_decay_sec <= 86400:
        raise ConfigError("market_maker.adaptive_spread_idle_decay_sec 必须在 0..86400")
    if not 1800 <= market_maker.adaptive_spread_sample_max_age_sec <= 604800:
        raise ConfigError(
            "market_maker.adaptive_spread_sample_max_age_sec 必须在 1800..604800"
        )

    g = _section(raw, "grid", {
        "anchor_price", "levels_each_side", "spacing_bps", "order_notional_usd",
        "min_profit_after_projected_rebate_bps", "breakout_buffer_bps",
        "recenter_when_flat_bps", "level_rearm_bps", "inventory_direction"
    })
    anchor = g.get("anchor_price")
    if anchor is not None and (isinstance(anchor, bool) or not isinstance(anchor, (int, float)) or not math.isfinite(anchor) or anchor <= 0):
        raise ConfigError("grid.anchor_price 必须为 null 或正数")
    grid = GridConfig(
        anchor_price=float(anchor) if anchor is not None else None,
        levels_each_side=int(_need(g, "levels_each_side", int)),
        spacing_bps=float(_need(g, "spacing_bps", (int, float))),
        order_notional_usd=float(_need(g, "order_notional_usd", (int, float))),
        min_profit_after_projected_rebate_bps=float(
            _need(g, "min_profit_after_projected_rebate_bps", (int, float))
        ),
        breakout_buffer_bps=float(_need(g, "breakout_buffer_bps", (int, float))),
        recenter_when_flat_bps=float(_need(g, "recenter_when_flat_bps", (int, float))),
        level_rearm_bps=float(_need(g, "level_rearm_bps", (int, float))),
        inventory_direction=g.get("inventory_direction", "long"),
    )
    if not 1 <= grid.levels_each_side <= 50:
        raise ConfigError("grid.levels_each_side 必须在 1..50")
    if grid.inventory_direction not in {"long", "short"}:
        raise ConfigError("grid.inventory_direction 必须为 long 或 short")
    for name in ("spacing_bps", "order_notional_usd", "level_rearm_bps"):
        _positive(f"grid.{name}", getattr(grid, name))
    for name in ("min_profit_after_projected_rebate_bps", "breakout_buffer_bps", "recenter_when_flat_bps"):
        _positive(f"grid.{name}", getattr(grid, name), allow_zero=True)
    if grid.spacing_bps * grid.levels_each_side >= 10_000:
        raise ConfigError("网格最下层价格必须大于 0，请减少层数或间距")

    rt = _section(raw, "runtime", {
        "loop_interval_sec", "account_refresh_sec", "fill_refresh_sec",
        "heartbeat_sec", "summary_refresh_sec", "cancel_strategy_orders_on_start",
        "cancel_strategy_orders_on_exit", "pause_during_us_regular_session",
        "us_regular_session_pre_open_buffer_min",
        "us_regular_session_post_close_buffer_min", "state_dir", "log_dir",
        "use_ws_user_feeds", "rest_sweep_sec", "use_batch_modify",
        "budget_poll_sec", "budget_soft_remaining", "budget_hard_remaining",
        "auto_reserve_actions", "reserve_actions_chunk",
        "reserve_actions_daily_max_usd"
    })
    base = source.parent
    pause_regular = rt.get("pause_during_us_regular_session", False)
    pre_open_buffer = rt.get("us_regular_session_pre_open_buffer_min", 5)
    post_close_buffer = rt.get("us_regular_session_post_close_buffer_min", 5)
    if not isinstance(pause_regular, bool):
        raise ConfigError("pause_during_us_regular_session 必须是布尔值")
    for name, value in (
        ("us_regular_session_pre_open_buffer_min", pre_open_buffer),
        ("us_regular_session_post_close_buffer_min", post_close_buffer),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 120:
            raise ConfigError(f"{name} 必须是 0..120 的整数")
    for name in ("use_ws_user_feeds", "use_batch_modify", "auto_reserve_actions"):
        if name in rt and not isinstance(rt[name], bool):
            raise ConfigError(f"runtime.{name} 必须是布尔值")
    runtime = RuntimeConfig(
        loop_interval_sec=float(_need(rt, "loop_interval_sec", (int, float))),
        account_refresh_sec=float(_need(rt, "account_refresh_sec", (int, float))),
        fill_refresh_sec=float(_need(rt, "fill_refresh_sec", (int, float))),
        heartbeat_sec=float(_need(rt, "heartbeat_sec", (int, float))),
        summary_refresh_sec=float(
            _need(rt, "summary_refresh_sec", (int, float))
        ),
        cancel_strategy_orders_on_start=_need(
            rt, "cancel_strategy_orders_on_start", bool
        ),
        cancel_strategy_orders_on_exit=_need(
            rt, "cancel_strategy_orders_on_exit", bool
        ),
        pause_during_us_regular_session=pause_regular,
        us_regular_session_pre_open_buffer_min=pre_open_buffer,
        us_regular_session_post_close_buffer_min=post_close_buffer,
        state_dir=(base / _need(rt, "state_dir", str)).resolve(),
        log_dir=(base / _need(rt, "log_dir", str)).resolve(),
        use_ws_user_feeds=rt.get("use_ws_user_feeds", True),
        rest_sweep_sec=float(rt.get("rest_sweep_sec", 60.0)),
        use_batch_modify=rt.get("use_batch_modify", True),
        budget_poll_sec=float(rt.get("budget_poll_sec", 60.0)),
        budget_soft_remaining=int(rt.get("budget_soft_remaining", 2500)),
        budget_hard_remaining=int(rt.get("budget_hard_remaining", 800)),
        auto_reserve_actions=rt.get("auto_reserve_actions", False),
        reserve_actions_chunk=int(rt.get("reserve_actions_chunk", 10_000)),
        reserve_actions_daily_max_usd=float(
            rt.get("reserve_actions_daily_max_usd", 10.0)
        ),
    )
    for name, value in (
        ("loop_interval_sec", runtime.loop_interval_sec),
        ("account_refresh_sec", runtime.account_refresh_sec),
        ("fill_refresh_sec", runtime.fill_refresh_sec),
        ("heartbeat_sec", runtime.heartbeat_sec),
        ("summary_refresh_sec", runtime.summary_refresh_sec),
        ("rest_sweep_sec", runtime.rest_sweep_sec),
        ("budget_poll_sec", runtime.budget_poll_sec),
    ):
        _positive(name, value)
    if runtime.budget_hard_remaining >= runtime.budget_soft_remaining:
        raise ConfigError("budget_hard_remaining 必须低于 budget_soft_remaining")
    if runtime.budget_hard_remaining < 0:
        raise ConfigError("budget_hard_remaining 不得为负数")
    if not 100 <= runtime.reserve_actions_chunk <= 200_000:
        raise ConfigError("reserve_actions_chunk 必须在 100..200000")
    _positive(
        "reserve_actions_daily_max_usd",
        runtime.reserve_actions_daily_max_usd,
        allow_zero=True,
    )

    p = _section(raw, "paper", {"initial_equity_usd", "touch_fill"})
    paper = PaperConfig(
        initial_equity_usd=float(_need(p, "initial_equity_usd", (int, float))),
        touch_fill=_need(p, "touch_fill", bool),
    )
    _positive("paper.initial_equity_usd", paper.initial_equity_usd)
    live_raw = _section(
        raw,
        "live",
        {"private_key_env", "account_address_env", "credentials_file", "wallet_id", "env_file"},
    )
    credentials_file = live_raw.get("credentials_file", "wallet.local.json")
    if not isinstance(credentials_file, str) or not credentials_file.strip():
        raise ConfigError("live.credentials_file 必须是非空字符串")
    wallet_id = live_raw.get("wallet_id")
    if wallet_id is not None and (
        not isinstance(wallet_id, str) or not wallet_id.strip()
    ):
        raise ConfigError("live.wallet_id 必须为 null 或非空字符串")
    env_file = live_raw.get("env_file")
    if env_file is not None and (not isinstance(env_file, str) or not env_file.strip()):
        raise ConfigError("live.env_file 必须为 null 或非空字符串")
    for name in ("private_key_env", "account_address_env"):
        if not _need(live_raw, name, str).strip():
            raise ConfigError(f"live.{name} 必须为非空字符串")
    live = LiveConfig(
        private_key_env=_need(live_raw, "private_key_env", str),
        account_address_env=_need(live_raw, "account_address_env", str),
        credentials_file=(base / credentials_file).resolve(),
        wallet_id=wallet_id.strip() if isinstance(wallet_id, str) else None,
        env_file=(base / env_file).resolve() if env_file is not None else None,
    )

    if strategy == "market_maker":
        if market_maker.order_notional_usd <= risk.min_order_notional_usd:
            raise ConfigError(
                "market_maker.order_notional_usd 必须高于最小订单金额，"
                "为数量步进向下取整预留空间"
            )
        if risk.max_open_order_usd < 2 * market_maker.order_notional_usd:
            raise ConfigError(
                "max_open_order_usd 必须至少容纳双边做市梯子的总预算"
            )
    else:
        if grid.order_notional_usd <= risk.min_order_notional_usd:
            raise ConfigError(
                "grid.order_notional_usd 必须高于最小订单金额，"
                "为数量步进向下取整预留空间"
            )
        if risk.max_open_order_usd < 2 * grid.order_notional_usd:
            raise ConfigError("max_open_order_usd 必须至少容纳网格双边各一笔订单")

    return Config(
        mode=mode,
        strategy=strategy,
        market=market,
        fees=fees,
        risk=risk,
        market_maker=market_maker,
        grid=grid,
        runtime=runtime,
        paper=paper,
        live=live,
        source_path=source,
    )
