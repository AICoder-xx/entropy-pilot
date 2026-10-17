"""Pure budget edits and offline validation for the local dashboard."""
from __future__ import annotations

import copy
import json
import math
import tempfile
from dataclasses import replace
from pathlib import Path

from entropy_bot.config import Config, ConfigError, load_config
from entropy_bot.sizing import scale_config_for_equity


FIELD_PATHS = {
    "order_notional_usd": ("market_maker", "order_notional_usd"),
    "max_position_usd": ("risk", "max_position_usd"),
    "max_open_order_usd": ("risk", "max_open_order_usd"),
    "max_drawdown_usd": ("risk", "max_session_drawdown_usd"),
}


class BudgetValidationError(ValueError):
    """Only fixed, safe labels belong in errors shown to the browser."""


def parse_budget_request(payload: object, symbols: tuple[str, ...]) -> tuple[str, dict]:
    if not isinstance(payload, dict) or set(payload) != {"symbol", "values", "fixed_budget"}:
        raise BudgetValidationError("预算请求必须包含市场、四项预算和固定预算确认")
    symbol = payload["symbol"]
    if not isinstance(symbol, str) or symbol not in symbols:
        raise BudgetValidationError("请选择支持的市场")
    if payload["fixed_budget"] is not True:
        raise BudgetValidationError("请使用“保存固定预算”，明确关闭该市场的权益自动放大")
    raw = payload["values"]
    if not isinstance(raw, dict) or set(raw) != set(FIELD_PATHS):
        raise BudgetValidationError("请填写完整的四项预算")
    values = {}
    for name in FIELD_PATHS:
        value = raw[name]
        try:
            valid = not isinstance(value, bool) and isinstance(value, (int, float)) and math.isfinite(value) and value > 0
        except OverflowError:
            valid = False
        if not valid:
            raise BudgetValidationError("四项预算必须为大于零的有限数值，不能使用文字或布尔值")
        values[name] = float(value)
    return symbol, values


def with_fixed_budget(settings: dict, symbol: str, values: dict) -> dict:
    candidate = copy.deepcopy(settings)
    if not isinstance(candidate, dict):
        raise BudgetValidationError("面板预算配置格式无效，请先修复配置文件")
    markets = candidate.setdefault("markets", {})
    if not isinstance(markets, dict):
        raise BudgetValidationError("面板市场配置格式无效")
    market = markets.setdefault(symbol, {})
    if not isinstance(market, dict):
        raise BudgetValidationError("所选市场配置格式无效")
    for name, (section, key) in FIELD_PATHS.items():
        target = market.setdefault(section, {})
        if not isinstance(target, dict):
            raise BudgetValidationError("所选市场预算配置格式无效")
        target[key] = values[name]
    market["risk"]["auto_scale_with_equity"] = False
    return candidate


def tuning_budget_conflict(tuning: dict) -> bool:
    paths = list(FIELD_PATHS.values()) + [("risk", "auto_scale_with_equity")]
    return any(isinstance(tuning.get(section), dict) and key in tuning[section] for section, key in paths)


def values_from_config(cfg: Config) -> dict:
    return {name: getattr(getattr(cfg, section), key) for name, (section, key) in FIELD_PATHS.items()}


def budget_preview(base: Config, payload: dict) -> tuple[Config, dict]:
    """Read-only sizing preview; never loads credentials or creates files."""
    cfg = replace(base, strategy="market_maker",
                  risk=replace(base.risk, **payload["risk"]),
                  market_maker=replace(base.market_maker, **payload["market_maker"]),
                  grid=replace(base.grid, **payload["grid"]))
    capital = cfg.risk.capital_usd
    maximum = cfg.risk.auto_scale_max_equity_usd
    return cfg, {
        "at_capital": values_from_config(scale_config_for_equity(cfg, capital)),
        "at_max_equity": values_from_config(scale_config_for_equity(cfg, maximum)),
        "capital_usd": capital,
        "max_equity_usd": maximum,
    }


def validate_budget_relations(values: dict, cfg: Config) -> None:
    order = values["order_notional_usd"]
    if order <= cfg.risk.min_order_notional_usd:
        raise BudgetValidationError("做市单侧预算必须高于最小订单金额，并留出数量取整余量")
    if values["max_position_usd"] < order:
        raise BudgetValidationError("最大净仓位不能低于做市单侧预算")
    if 2 * cfg.market_maker.crisis_quote_notional_usd > order:
        raise BudgetValidationError("危机减仓报价可达危机预算两倍，超过新做市单侧预算；请先降低危机报价参数或提高做市单侧预算")
    if values["max_open_order_usd"] < 2 * max(order, cfg.grid.order_notional_usd):
        raise BudgetValidationError("最大挂单需同时容纳双侧做市预算，并至少容纳网格两笔订单")


def validate_complete_payload(payload: dict) -> None:
    """Use the production parser before saving either strategy's next config."""
    with tempfile.TemporaryDirectory(prefix="entropy-budget-validation-") as directory:
        path = Path(directory) / "config.json"
        for strategy in ("market_maker", "grid"):
            candidate = copy.deepcopy(payload)
            candidate["strategy"] = strategy
            path.write_text(json.dumps(candidate, allow_nan=False), encoding="utf-8")
            try:
                load_config(path)
            except (ConfigError, ValueError, TypeError, KeyError, OverflowError):
                raise BudgetValidationError("合并后的完整策略配置未通过校验，请检查现有配置与预算关系") from None
