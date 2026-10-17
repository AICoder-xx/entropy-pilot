"""Public configuration API and file/domain orchestration."""
from __future__ import annotations

import json
from pathlib import Path

from .config_schema import (
    ConfigError as ConfigError,
    MarketConfig as MarketConfig,
    FeeConfig as FeeConfig,
    RiskConfig as RiskConfig,
    MarketMakerConfig as MarketMakerConfig,
    GridConfig as GridConfig,
    RuntimeConfig as RuntimeConfig,
    PaperConfig as PaperConfig,
    LiveConfig as LiveConfig,
    Config as Config,
    _need,
)
from .config_sections import (
    parse_market,
    parse_fees,
    parse_risk,
    parse_market_maker,
    parse_grid,
    parse_runtime,
    parse_paper,
    parse_live,
)


ROOT_KEYS = {
    "mode", "strategy", "market", "fees", "risk", "market_maker", "grid",
    "runtime", "paper", "live", "_说明"
}


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

    market = parse_market(raw)
    fees = parse_fees(raw)
    risk = parse_risk(raw)
    market_maker = parse_market_maker(raw)
    grid = parse_grid(raw)
    runtime = parse_runtime(raw, source.parent)
    paper = parse_paper(raw)
    live = parse_live(raw, source.parent)
    _validate_strategy_budget(strategy, risk, market_maker, grid)

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


def _validate_strategy_budget(
    strategy: str, risk: RiskConfig, market_maker: MarketMakerConfig, grid: GridConfig,
) -> None:
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
