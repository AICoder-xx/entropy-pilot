"""Account/inventory risk evaluation independent of the order executor.

Inputs are explicit observations. The only mutable input is rolling equity
history; this evaluator never cancels, signs or submits an exchange action.
"""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass
from typing import Callable

from .config import Config
from .models import AccountState, MarketSnapshot, TrackedOrder


@dataclass(frozen=True)
class RiskView:
    cfg: Config
    snapshot: MarketSnapshot
    account: AccountState
    orders: dict[str, TrackedOrder]
    official_untracked_bot_orders: int
    official_foreign_orders: int
    official_leverage_type: str | None
    official_leverage_value: int | None
    start_equity: float
    consecutive_errors: int
    equity_history: deque[tuple[float, float]]
    daily_budget_baseline: Callable[[float], float]
    strategy_halt_reason: Callable[[MarketSnapshot, AccountState], str | None]
    liquidation_min_distance: float


def _official_state_reason(view: RiskView) -> str | None:
    if view.cfg.mode == "live":
        bot_orphans = int(
            getattr(view, "official_untracked_bot_orders", 0) or 0
        )
        if bot_orphans:
            return f"检测到 {bot_orphans} 笔本机器人遗留挂单未纳入当前映射"
        foreign_orders = int(
            getattr(view, "official_foreign_orders", 0) or 0
        )
        if foreign_orders:
            return (
                f"检测到 {foreign_orders} 笔同市场非本进程挂单；"
                "为避免与外部交易并行已停机"
            )
        target = view.cfg.market.target_leverage
        if target is not None:
            kind = getattr(view, "official_leverage_type", None)
            value = getattr(view, "official_leverage_value", None)
            if kind is not None or value is not None:
                if kind != "isolated" or value != target:
                    return (
                        f"官方杠杆偏离配置: {kind or 'unknown'} "
                        f"{value or 0}x，要求 isolated {target}x"
                    )
            elif abs(view.account.position_qty) >= view.snapshot.meta.size_step / 2:
                return "存在持仓但无法核验官方逐仓杠杆"
    return None


def _inventory_reason(view: RiskView, fair: float) -> str | None:
    risk = view.cfg.risk
    position_usd = abs(view.account.position_qty * fair)
    if position_usd > risk.max_position_usd * 1.02:
        position_qty = view.account.position_qty
        orders = list(view.orders.values())
        only_reducing = all(
            order.reduce_only
            and (
                (position_qty < 0 and order.is_buy)
                or (position_qty > 0 and not order.is_buy)
            )
            for order in orders
        )
        recovery_ceiling = (
            risk.max_position_usd
            + view.cfg.market_maker.order_notional_usd * 1.10
        )
        if not (
            view.cfg.strategy == "market_maker"
            and position_usd <= recovery_ceiling
            and only_reducing
        ):
            return (
                f"持仓 ${position_usd:.2f} 超过上限 "
                f"${risk.max_position_usd:.2f}"
            )
    liquidation_px = getattr(view.account, "liquidation_px", None)
    mark = getattr(view.snapshot, "mark_px", None) or fair
    if (
        liquidation_px
        and mark
        and position_usd > risk.min_order_notional_usd
    ):
        # Margin backstop.  At 3X isolated with the 60%-of-equity position
        # cap the liquidation price sits ~25-30% away from mark; anything
        # inside this floor means leverage, margin or position state is
        # not what this configuration assumes.  Halting flattens the
        # position (flatten_on_halt) long before the exchange can.
        distance_pct = abs(mark - liquidation_px) / mark
        if distance_pct < view.liquidation_min_distance:
            return (
                f"强平价距离仅 {distance_pct * 100:.1f}%"
                f"（低于 {view.liquidation_min_distance * 100:.0f}% 保护线），"
                "杠杆/保证金状态异常"
            )
    open_usd = sum(x.price * (x.qty - x.filled_qty) for x in view.orders.values())
    if open_usd > risk.max_open_order_usd * 1.02:
        return f"挂单 ${open_usd:.2f} 超过上限 ${risk.max_open_order_usd:.2f}"
    return None


def _loss_budget_reason(view: RiskView) -> str | None:
    risk = view.cfg.risk
    principal_drawdown = risk.capital_usd - view.account.equity_actual
    if principal_drawdown >= risk.max_principal_drawdown_usd:
        return (
            f"本金累计回撤 ${principal_drawdown:.2f} 达到停机线 "
            f"${risk.max_principal_drawdown_usd:.2f}"
        )
    if view.start_equity > 0:
        drawdown = view.start_equity - view.account.equity_actual
        if drawdown >= risk.max_session_drawdown_usd:
            return f"实际权益回撤 ${drawdown:.2f} 达到停机线"
    equity = view.account.equity_actual
    if risk.max_rolling_drawdown_usd > 0 and equity > 0:
        # Incident line: losing this much inside a short window is not
        # ordinary wear (a one-way move, a bug, a pickoff storm).
        now = time.monotonic()
        window_sec = risk.rolling_drawdown_window_min * 60.0
        view.equity_history.append((now, equity))
        while (
            view.equity_history
            and view.equity_history[0][0] < now - window_sec
        ):
            view.equity_history.popleft()
        burst = max(item[1] for item in view.equity_history) - equity
        if burst >= risk.max_rolling_drawdown_usd:
            return (
                f"{risk.rolling_drawdown_window_min:.0f} 分钟内快速回撤 "
                f"${burst:.2f} 达到事故线 ${risk.max_rolling_drawdown_usd:.2f}"
            )
    if risk.daily_loss_budget_usd > 0 and equity > 0:
        # Budget line: slow accumulated wear is an operating cost, not an
        # incident.  The baseline is shared across both market processes
        # (same account equity) and survives restarts within the day, so
        # a restart cannot refill the budget.
        spent = view.daily_budget_baseline(equity) - equity
        if spent >= risk.daily_loss_budget_usd:
            return (
                f"当日亏损预算已用尽（${spent:.2f}/"
                f"${risk.daily_loss_budget_usd:.2f}，属正常磨损成本上限；"
                "次日重新核验后可手动启动）"
            )
    return None


def risk_reason(view: RiskView) -> str | None:
    """Keep original guard precedence while separating observable concerns."""
    reason = _official_state_reason(view)
    if reason:
        return reason
    # Feed freshness remains a separate quarantine; it never relaxes these
    # account, inventory, loss-budget and collateral checks.
    fair = view.snapshot.fair_px
    if fair is None:
        return "无法计算 fair price"
    reason = _inventory_reason(view, fair) or _loss_budget_reason(view)
    if reason:
        return reason
    risk = view.cfg.risk
    if view.cfg.mode == "live" and risk.min_free_collateral_usd > 0:
        free = view.account.withdrawable
        if free is None:
            return "无法核实可用 USDC，按 fail-closed 停机"
        if free < risk.min_free_collateral_usd:
            return f"可用 USDC ${free:.2f} 低于保留线 ${risk.min_free_collateral_usd:.2f}"
    if view.consecutive_errors >= risk.max_consecutive_errors:
        return f"连续错误达到 {view.consecutive_errors} 次"
    return view.strategy_halt_reason(view.snapshot, view.account)
