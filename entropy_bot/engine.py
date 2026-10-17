from __future__ import annotations

import asyncio
import hashlib
import logging
import math
import time
import uuid
from collections import deque
from pathlib import Path

import aiohttp

from .broker import (
    Broker,
    LiveBroker,
    PaperBroker,
    PendingOrderReconciliation,
    UncertainIocOutcome,
    CANCELED_ORDER_STATES,
    _order_state,
    recovered_order_identity,
    allocation_state,
)
from .config import Config
from .credentials import resolve_live_credentials
from .fees import build_fee_model
from .grid_runtime import GridRuntime
from .instance_lock import InstanceLock
from .market import MarketClient, build_rest_session
from .models import FeeModel, MarketSnapshot, OrderIntent, TrackedOrder
from .storage import Ledger, atomic_write_json, read_json
from .strategies import Strategy, make_strategy
from .sizing import (
    effective_risk_profile,
    equity_step_reference,
    scale_config_for_equity,
)
from .trading_schedule import TradingScheduleState, us_regular_session_state

log = logging.getLogger("entropy.engine")
BOOK_RECONNECT_GRACE_SEC = 15.0
SCHEDULE_ORDER_AUDIT_SEC = 60.0
CONNECTIVITY_RETRY_MAX_SEC = 30.0
# Fraction of mark price the liquidation price must stay away from.  At 5X
# isolated with the configured position caps the true distance is ~18%;
# anything inside this floor means leverage/margin state is abnormal.
LIQUIDATION_GUARD_MIN_DISTANCE = 0.10
# Emergency flatten on halt: bounded retries with a book refresh between
# attempts, so a transient network drop cannot leave inventory unmanaged.
FLATTEN_RETRY_ATTEMPTS = 3
FLATTEN_RETRY_DELAY_SEC = 10.0
# Inventory aging: a position stuck past the rebalance threshold
# for this long is actively reduced with bounded-slippage IOC clips.
# The clip limits constrain execution cost while reducing exposure.
INVENTORY_IOC_AFTER_SEC = 300.0
INVENTORY_IOC_INTERVAL_SEC = 60.0
INVENTORY_IOC_SLIPPAGE_BPS = 15.0
RECOVERY_HOLD_REASON = (
    "恢复隔离：进程仅监控且禁止挂单；等待下一次美股常规交易时段保护后自动释放"
)


class Engine:
    def __init__(
        self,
        cfg: Config,
        *,
        control_file: Path | None = None,
        run_id: str | None = None,
    ) -> None:
        self.cfg = cfg
        self.base_cfg = cfg
        self.control_file = control_file
        self.control_run_id = run_id
        self.stop = asyncio.Event()
        self.book_event = asyncio.Event()
        self.session: aiohttp.ClientSession | None = None
        self.market: MarketClient | None = None
        self.snapshot: MarketSnapshot | None = None
        self.fees: FeeModel | None = None
        self.broker: Broker | None = None
        self.strategy: Strategy | None = None
        self.ledger: Ledger | None = None
        self.grid_runtime: GridRuntime | None = None
        self._instance_lock: InstanceLock | None = None
        self.start_equity = 0.0
        self.start_market_unrealized = 0.0
        self.status = "initializing"
        self.halt_reason_text: str | None = None
        self._errors = 0
        self._last_context = 0.0
        self._last_sync = 0.0
        self._last_reconcile = 0.0
        self._last_heartbeat = 0.0
        self._last_summary = 0.0
        self._ever_had_book = False
        # Loss-velocity guards: rolling equity history for the incident line
        # and the shared per-day baseline for the wear budget.
        self._equity_history: deque[tuple[float, float]] = deque()
        self._daily_budget_date: str | None = None
        self._daily_budget_start = 0.0
        # A dashboard-owned run id is also the ledger session id.  This gives
        # the controller an exact, race-free summary binding without having
        # the child rewrite the shared control file during shutdown.
        self.session_id = self.control_run_id or uuid.uuid4().hex[:16]
        self.session_started_at_ms = int(time.time() * 1000)
        self._pending_markouts: dict[str, dict] = {}
        self._mid_history: deque[tuple[int, float]] = deque(maxlen=60_000)
        self._schedule_paused = False
        self._schedule_reason: str | None = None
        self._last_schedule_cancel_attempt = 0.0
        # Market-quality pause: a wide/empty book or oracle drift is a market
        # state (overnight liquidity, oracle updater lag), not account risk.
        # Quoting pauses and resumes with hysteresis instead of halting.
        self._market_quality_paused = False
        self._market_quality_reason: str | None = None
        self._last_market_quality_cancel = 0.0
        # Oracle liveness for crisis quoting: the anchor is only trustworthy
        # if its VALUE moved recently (a weekend equity oracle keeps pushing
        # the stale Friday close, which must never anchor far-touch orders).
        self._last_oracle_px: float | None = None
        self._oracle_changed_at = 0.0
        self._last_mq_snapshot = 0.0
        # Inventory aging (stage 3): time the position first breached the
        # rebalance threshold, and the last active-reduction clip.
        self._inventory_breach_since: float | None = None
        self._last_inventory_ioc = 0.0
        self._book_stale_since: float | None = None
        self._book_stale_reason: str | None = None
        self._last_book_stale_log = 0.0
        self._sizing_equity_reference = cfg.risk.capital_usd
        self.wallet_id = cfg.live.wallet_id or "primary"

    def _apply_dynamic_sizing(self) -> bool:
        if not self.broker or not self.base_cfg.risk.auto_scale_with_equity:
            return False
        candidate = equity_step_reference(
            self.base_cfg, self.broker.account.equity_actual
        )
        reference = max(self._sizing_equity_reference, candidate)
        new_cfg = scale_config_for_equity(self.base_cfg, reference)
        old_profile = effective_risk_profile(self.cfg)
        new_profile = effective_risk_profile(new_cfg)
        self._sizing_equity_reference = reference
        if new_profile == old_profile:
            return False
        self.cfg = new_cfg
        self.broker.cfg = new_cfg
        if self.strategy is not None:
            strategy_state = self.strategy.state_dict()
            self.strategy.cfg = new_cfg
            # A sizing step changes quote participation and inventory ratios.
            # Re-load through the strategy's regime guard so adaptive samples
            # from the old capital profile cannot leak into the new one.
            self.strategy.load_state(strategy_state)
        if self.control_file is not None and self.control_run_id is not None:
            control = read_json(self.control_file, None, strict=False)
            if (
                isinstance(control, dict)
                and control.get("run_id") == self.control_run_id
            ):
                control["risk_profile"] = new_profile
                atomic_write_json(self.control_file, control)
        log.warning(
            "权益阶梯定仓更新：基准=$%.2f 每单=$%.2f 单标的仓位上限=$%.2f 挂单上限=$%.2f",
            new_profile["configured_capital_usd"],
            new_profile["order_notional_usd"],
            new_profile["max_position_usd"],
            new_profile["max_open_order_usd"],
        )
        return True

    def _external_stop_reason(self) -> str | None:
        if self.control_file is None or self.control_run_id is None:
            return None
        value = read_json(self.control_file, None, strict=False)
        if not isinstance(value, dict):
            return "UI 控制文件不可读"
        if value.get("run_id") != self.control_run_id:
            return "UI 运行所有权已变化"
        if value.get("desired_state") != "running":
            return "UI 已请求停止"
        heartbeat_ms = value.get("dashboard_heartbeat_ms")
        if not isinstance(heartbeat_ms, (int, float)):
            return "UI 心跳缺失"
        if int(time.time() * 1000) - int(heartbeat_ms) > 15_000:
            return "UI 心跳超过 15 秒"
        return None

    def notify_book(self) -> None:
        if self.market is not None:
            mid = self.market.book.mid
            if mid is not None:
                self._mid_history.append((int(time.time() * 1000), mid))
        self.book_event.set()

    def _trading_schedule_state(self) -> TradingScheduleState:
        return us_regular_session_state(
            enabled=self.cfg.runtime.pause_during_us_regular_session,
            pre_open_buffer_min=(
                self.cfg.runtime.us_regular_session_pre_open_buffer_min
            ),
            post_close_buffer_min=(
                self.cfg.runtime.us_regular_session_post_close_buffer_min
            ),
        )

    def _recovery_hold_requested(self) -> bool:
        """Return the persisted fail-closed recovery intent for this run."""

        if self.control_file is None or self.control_run_id is None:
            return False
        value = read_json(self.control_file, None, strict=False)
        return bool(
            isinstance(value, dict)
            and value.get("run_id") == self.control_run_id
            and value.get("recovery_hold_until_session_pause") is True
        )

    def _release_recovery_hold(self) -> None:
        """Persist release only after an actual configured session pause."""

        if self.control_file is None or self.control_run_id is None:
            return
        value = read_json(self.control_file, None, strict=False)
        if not (
            isinstance(value, dict)
            and value.get("run_id") == self.control_run_id
            and value.get("recovery_hold_until_session_pause") is True
        ):
            return
        value["recovery_hold_until_session_pause"] = False
        value["recovery_hold_released_at_ms"] = int(time.time() * 1000)
        value["message"] = "恢复隔离已解除；当前仍由交易时段保护保持零挂单"
        atomic_write_json(self.control_file, value)
        log.warning("已亲历美股交易暂停窗口；恢复隔离已解除，继续等待收盘后自动恢复")

    def _effective_trading_schedule_state(self) -> TradingScheduleState:
        """Overlay the one-shot recovery hold on the ordinary schedule gate.

        A hard-stopped bot may be made observable again outside the official
        recovery window, but it must not quote merely because a weekend is
        ordinarily tradable.  The hold is released only while the real
        schedule gate is active; ordinary post-close recovery then performs
        the existing full sync, risk check and reconciliation before quoting.
        """

        state = self._trading_schedule_state()
        if not self._recovery_hold_requested():
            return state
        if state.paused:
            self._release_recovery_hold()
            return state
        return TradingScheduleState(
            paused=True,
            reason=RECOVERY_HOLD_REASON,
            eastern_now=state.eastern_now,
            session_open=state.session_open,
            session_close=state.session_close,
        )

    async def _enter_schedule_pause(self, state: TradingScheduleState) -> None:
        first_entry = not self._schedule_paused
        self._schedule_paused = True
        self._schedule_reason = state.reason or "美股常规交易时段暂停"
        self.status = "paused"
        now = time.monotonic()
        if first_entry:
            log.warning("%s", self._schedule_reason)
        attempted_cancel = first_entry or (
            self.broker
            and self.broker.orders_by_role
            and now - self._last_schedule_cancel_attempt >= 30.0
        ) or (
            self.cfg.mode == "live"
            and now - self._last_schedule_cancel_attempt >= SCHEDULE_ORDER_AUDIT_SEC
        )
        if attempted_cancel:
            self._last_schedule_cancel_attempt = now
            clean = await self._cancel_and_reconcile_exit("交易时段暂停")
            if not clean:
                log.error("交易时段暂停撤单尚未完全确认；保持禁止新挂单并继续重试")
        if first_entry or attempted_cancel:
            self._write_summary("paused", self._schedule_reason)

    def _recovery_hold_invariant_reason(
        self, state: TradingScheduleState
    ) -> str | None:
        """Keep a recovery hold observable without allowing latent exposure.

        The account snapshot has already been refreshed before this check.  A
        recovery hold is intentionally stricter than an ordinary scheduled
        pause: both strategy orders and the market position must be zero.
        """

        if state.reason != RECOVERY_HOLD_REASON:
            return None
        assert self.broker and self.snapshot
        if self.broker.orders_by_role:
            return "恢复隔离要求本地挂单为 0"
        if abs(self.broker.account.position_qty) >= self.snapshot.meta.size_step / 2:
            return "恢复隔离要求本地持仓为 0"
        return None

    async def _fail_recovery_hold_invariant(self, reason: str) -> None:
        """Fail closed without letting ``flatten_on_halt`` submit a trade."""

        message = f"恢复隔离检查失败: {reason}"
        self.status = "halted"
        self.halt_reason_text = message
        log.error("风险停机: %s", message)
        self._write_summary("halted", message)
        self.stop.set()

    def _market_quality_pause_reason(self) -> str | None:
        """Pause instead of halting while the market itself is not quotable.

        A wide or empty book and mid-price drift from the oracle call for
        temporarily standing aside while continuing account monitoring.
        Hysteresis prevents repeated transitions at the pause boundary.
        """
        assert self.broker and self.snapshot
        risk = self.cfg.risk
        book = self.snapshot.book
        hysteresis = 0.8 if self._market_quality_paused else 1.0
        spread = book.spread_bps
        if spread is None:
            return "盘口为空（无可参考买卖盘），暂停报价等待流动性恢复"
        if spread > risk.max_book_spread_bps * hysteresis:
            return (
                f"盘口价差 {spread:.1f} bps 超过 "
                f"{risk.max_book_spread_bps * hysteresis:.1f} bps"
                "（流动性稀薄，暂停报价等待恢复）"
            )
        if book.mid and self.snapshot.oracle_px:
            deviation = abs(book.mid / self.snapshot.oracle_px - 1.0) * 10_000
            if deviation > risk.max_oracle_deviation_bps * hysteresis:
                return (
                    f"盘口中价偏离 oracle {deviation:.2f} bps"
                    "（暂停报价等待收敛）"
                )
        return None

    def _track_oracle_liveness(self) -> None:
        oracle = self.snapshot.oracle_px if self.snapshot else None
        if oracle and oracle != self._last_oracle_px:
            self._last_oracle_px = oracle
            self._oracle_changed_at = time.monotonic()

    def _crisis_quotes_allowed(self) -> bool:
        """Crisis quoting needs a LIVE anchor, not merely a fresh push."""
        cfg = self.cfg.market_maker
        if (
            self.cfg.strategy != "market_maker"
            or cfg.crisis_quote_notional_usd < self.cfg.risk.min_order_notional_usd
        ):
            return False
        ctx_age = getattr(self.market, "ws_ctx_age_sec", None)
        if ctx_age is None or ctx_age > 90.0:
            return False
        return (
            self._oracle_changed_at > 0.0
            and time.monotonic() - self._oracle_changed_at <= 600.0
        )

    def _note_market_quality_snapshot(self, *, crisis: bool) -> None:
        """Sample the dislocated book into the order-event ledger (30 s cadence).

        This builds the offline evidence base for tuning crisis quoting:
        how wide the book got, where mid sat versus the oracle, how long the
        dislocation lasted."""
        now = time.monotonic()
        if now - self._last_mq_snapshot < 30.0 or not self.snapshot:
            return
        self._last_mq_snapshot = now
        book = self.snapshot.book
        if self.broker is not None:
            self.broker._note_order_event(
                "market_quality",
                spread_bps=book.spread_bps,
                best_bid=book.best_bid,
                best_ask=book.best_ask,
                mid=book.mid,
                oracle=self.snapshot.oracle_px,
                mark=self.snapshot.mark_px,
                crisis=crisis,
            )

    async def _enter_market_quality_pause(self, reason: str) -> None:
        first_entry = not self._market_quality_paused
        self._market_quality_paused = True
        self._market_quality_reason = reason
        self.status = "paused"
        now = time.monotonic()
        if first_entry:
            log.warning("市场状态暂停: %s", reason)
        needs_cancel = first_entry or (
            self.broker
            and self.broker.orders_by_role
            and now - self._last_market_quality_cancel >= 30.0
        )
        if needs_cancel:
            self._last_market_quality_cancel = now
            clean = await self._cancel_and_reconcile_exit("市场状态暂停")
            if not clean:
                log.error("市场状态暂停撤单尚未完全确认；保持禁止新挂单并继续重试")
        if first_entry or needs_cancel:
            self._write_summary("paused", reason)

    async def _resume_after_market_quality_pause(self) -> bool:
        assert self.market and self.broker
        self._market_quality_paused = False
        self._market_quality_reason = None
        self.status = "running"
        await self.broker.sync(self.market.book)
        reason = self._risk_reason()
        if reason:
            await self.halt(f"市场状态恢复前风险检查失败: {reason}")
            return False
        await self._reconcile(force=True)
        log.info("市场状态已恢复（盘口/oracle 回到正常范围），继续做市")
        self._write_summary("running")
        return True

    async def _resume_after_schedule_pause(self) -> bool:
        assert self.market and self.broker
        self._schedule_paused = False
        self._schedule_reason = None
        self.status = "running"
        await self.market.refresh_context()
        await self.broker.sync(self.market.book)
        reason = self._risk_reason()
        if reason:
            await self.halt(f"收盘后恢复前风险检查失败: {reason}")
            return False
        await self._reconcile(force=True)
        log.info("美股常规交易时段结束；做市已自动恢复")
        self._write_summary("running")
        return True

    async def _load_fee_model(self, address: str | None) -> FeeModel:
        assert self.market is not None and self.market.meta is not None
        user_fees = None
        if address:
            try:
                user_fees = await self.market.user_fees(address)
            except Exception as exc:
                if self.cfg.fees.fail_closed_if_user_fees_unavailable:
                    raise RuntimeError("无法读取账户真实 userFees，按配置拒绝启动") from exc
                log.warning("无法读取账户 userFees，使用保守基础费率")
        return build_fee_model(
            self.market.meta,
            user_fees,
            self_rebate_pct=self.cfg.fees.entropy_self_rebate_pct,
            referred_benefit_pct=self.cfg.fees.entropy_referred_user_benefit_pct,
        )

    async def initialize(self) -> None:
        self.session = build_rest_session()
        # Cheap open-order polling plus on-demand fill retrieval is about 276
        # info weight/minute for a quiet market and up to ~436 while every
        # cycle finds a fill. Keep each independent trader at 450 so the two
        # live markets cannot advertise more than ~900 aggregate weight/minute.
        # This preserves a small active-fill burst allowance while leaving
        # shared IP headroom for exchange state propagation and recovery calls.
        # Both live markets share Hyperliquid's per-IP REST allowance.  Keep
        # enough aggregate headroom for the dashboard and one bounded audit.
        self.market = MarketClient(self.cfg.market, self.session, info_capacity=350)
        meta = await self.market.load_meta()
        await asyncio.gather(self.market.refresh_context(), self.market.rest_book_once())
        self.snapshot = self.market.snapshot
        assert self.snapshot is not None

        address = None
        private_key = None
        if self.cfg.mode == "live":
            credentials = resolve_live_credentials(
                self.cfg, require_private_key=True
            )
            private_key = credentials.private_key
            address = credentials.account_address
            self.wallet_id = credentials.wallet_id
            if not address:
                raise RuntimeError("实盘缺少主账户/子账户地址")
            log.info(
                "钱包凭据已从 %s 加载；账户地址%s",
                credentials.source,
                "由私钥自动推导" if credentials.account_was_derived else "已显式配置",
            )
        else:
            credentials = resolve_live_credentials(
                self.cfg, require_private_key=False
            )
            address = credentials.account_address

        self.fees = await self._load_fee_model(address)
        identity = f"{self.cfg.mode}:{self.cfg.market.api_url}:{address or self.cfg.runtime.state_dir}:{meta.coin}"
        self._instance_lock = InstanceLock(
            Path(__file__).resolve().parents[1] / "state" / "process_locks", identity
        )
        self._instance_lock.acquire()
        self.strategy = make_strategy(self.cfg, self.fees)
        self.ledger = Ledger(
            self.cfg.runtime.state_dir,
            self.cfg.mode,
            self.cfg.strategy,
            meta.coin,
            self.session_id,
        )
        saved = self.ledger.load_strategy_state()
        if isinstance(saved, dict):
            self.strategy.load_state(saved)

        if self.cfg.mode == "live":
            assert private_key and address
            self.broker = LiveBroker(
                self.cfg, meta, self.fees, self.market, private_key, address
            )
        else:
            self.broker = PaperBroker(self.cfg, meta, self.fees)
        if self.cfg.mode == "live":
            # This precedes signer creation and includes unresolved intents
            # from the other strategy in the same wallet/market data root.
            await self._recover_order_intents()
            self.broker.order_intent_sink = self.ledger.record_order_intent
        if self.cfg.strategy == "grid":
            self.grid_runtime = GridRuntime(
                self.cfg, self.ledger, self.market, self.broker,
                hashlib.sha256(identity.encode()).hexdigest(),
            )
            def persist_grid_intent(row: dict) -> None:
                assert self.ledger is not None and self.grid_runtime is not None
                self.ledger.record_order_intent(row)
                self.grid_runtime.remember(row)
            self.broker.order_intent_sink = persist_grid_intent
            await self.grid_runtime.initialize()
            if not self.grid_runtime.ready():
                self._write_summary("paused", "网格库存账本未完成启动对账")
                raise RuntimeError("网格库存账本未完成启动对账；保留原始台账，未启动签名器")
        await self.broker.start()
        if (
            self.cfg.mode == "live"
            and self.cfg.runtime.cancel_strategy_orders_on_start
            and isinstance(self.broker, LiveBroker)
        ):
            count = await self.broker.cancel_bot_orphans()
            if count:
                log.warning("启动时撤销了 %d 笔本机器人遗留订单", count)
        await self.broker.sync(self.market.book)
        self._apply_dynamic_sizing()
        self.start_equity = self.broker.account.equity_actual
        self.start_market_unrealized = self.broker.account.unrealized_pnl
        self.status = "running"
        self._log_startup()

    async def _recover_order_intents(self) -> None:
        """Read-only crash recovery; absence can never authorize another send."""
        assert self.ledger and self.market and self.broker
        for path, intent in self.ledger.pending_order_intents():
            cloid = str(intent["cloid"])
            status = await self.market.order_status(self.broker.address, cloid)
            terminal = _order_state(status)
            oid = recovered_order_identity(status, cloid, self.broker.meta.coin)
            if oid is None or terminal not in {"open", "filled", "triggered", *CANCELED_ORDER_STATES}:
                raise RuntimeError(
                    "存在崩溃前未决下单意图，官方未提供匹配订单的确定结果；"
                    "保持 UNKNOWN，未启动签名器且禁止重复下单"
                )
            # GridRuntime still needs its own cumulative-fill reconciliation;
            # leave grid intents pending for that later inventory audit.
            if self.cfg.strategy == "grid" and path.resolve() == self.ledger.orders_path.resolve():
                continue
            self.ledger.record_recovered_order_intent(
                path, {**intent, "oid": oid, "outcome": "recovery_confirmed", "exchange_state": terminal}
            )

    def _log_startup(self) -> None:
        assert self.snapshot and self.fees and self.broker
        f = self.fees
        log.info(
            "%s %s %s | Growth=%s | bid/ask=%s/%s spread=%.4f bps",
            self.cfg.mode.upper(),
            self.cfg.strategy,
            self.snapshot.meta.coin,
            self.snapshot.meta.growth_mode,
            self.snapshot.book.best_bid,
            self.snapshot.book.best_ask,
            self.snapshot.book.spread_bps or 0.0,
        )
        log.info(
            "费率：gross maker/taker %.4f/%.4f bps；计入已配置 Entropy 权益后的投影 %.4f/%.4f bps",
            f.gross_maker_rate * 10_000,
            f.gross_taker_rate * 10_000,
            f.projected_net_maker_rate * 10_000,
            f.projected_net_taker_rate * 10_000,
        )
        if self.cfg.fees.entropy_referred_user_benefit_pct == 0:
            log.info("下级优惠未计入本账户收益（默认 0%，避免把会员卡的 75% 重复算给自己）")

    async def _book_recovery_pending(self) -> bool:
        """Quarantine quotes while the market feed is stale.

        With no resting orders, wait for the book to recover without
        escalating a feed outage into a halt. If orders are still resting,
        remain fail-closed after the configured grace window.
        """

        assert self.snapshot and self.broker
        if self.snapshot.book.is_fresh(self.cfg.market.max_book_age_sec):
            if self._book_stale_since is not None:
                log.info("盘口数据已恢复，继续做市")
                self.status = "running"
            self._book_stale_since = None
            self._book_stale_reason = None
            return False
        now = time.monotonic()
        if self._book_stale_since is None:
            self._book_stale_since = now
            stale_roles = list(self.broker.orders_by_role)
            log.warning(
                "盘口暂时不新鲜；先撤销 %d 张挂单并等待重连",
                len(stale_roles),
            )
            if stale_roles:
                await self.broker.cancel_roles(stale_roles)
        if now - self._book_stale_since < BOOK_RECONNECT_GRACE_SEC:
            return True
        if not self.broker.orders_by_role:
            self.status = "paused"
            self._book_stale_reason = "行情数据中断，挂单已清空，等待恢复后自动继续"
            if now - self._last_book_stale_log >= 60.0:
                self._last_book_stale_log = now
                log.warning(
                    "盘口持续陈旧 %.0f 秒；挂单为零，保持等待（不停机）",
                    now - self._book_stale_since,
                )
            return True
        # Stale feed with orders we could not confirm away: fail closed.
        return False

    @staticmethod
    def _is_transient_connectivity_error(exc: BaseException) -> bool:
        """Classify transport failures without swallowing programming errors."""

        current: BaseException | None = exc
        seen: set[int] = set()
        while current is not None and id(current) not in seen:
            seen.add(id(current))
            if isinstance(
                current,
                (
                    aiohttp.ClientConnectionError,
                    aiohttp.ServerTimeoutError,
                    asyncio.TimeoutError,
                    TimeoutError,
                ),
            ):
                return True
            if isinstance(current, aiohttp.ClientResponseError) and (
                current.status in {408, 425, 429}
                or 500 <= current.status <= 599
            ):
                return True
            current = current.__cause__ or current.__context__
        return False

    async def _recover_transient_connectivity(self, trigger: BaseException) -> bool:
        """Stay alive and fail closed until official state is authoritative.

        This path never places an order while connectivity is uncertain.  It
        repeatedly tries the normal idempotent cancel/reconciliation flow and
        resumes quoting only after a fresh book, account sync and every normal
        risk gate have passed.
        """

        assert self.market and self.snapshot and self.broker
        reason = "官方连接中断：已禁止新挂单，等待撤单与成交对账恢复"
        self.status = "paused"
        self._write_summary("paused", reason)
        log.error("%s: %s", reason, trigger)
        retry_sec = 1.0
        last_summary = 0.0
        while not self.stop.is_set():
            external_reason = self._external_stop_reason()
            if external_reason:
                log.warning("%s；连接隔离结束并按请求退出", external_reason)
                return False
            clean = await self._cancel_and_reconcile_exit(
                "连接隔离", timeout_sec=5.0, sleep_sec=0.5
            )
            foreign_orders = int(
                getattr(self.broker, "official_foreign_orders", 0) or 0
            )
            if foreign_orders:
                await self.halt(
                    f"连接恢复时发现 {foreign_orders} 笔同市场非本进程挂单"
                )
                return False
            if clean:
                try:
                    schedule_state = self._effective_trading_schedule_state()
                    if schedule_state.paused:
                        await self._enter_schedule_pause(schedule_state)
                        hold_reason = self._recovery_hold_invariant_reason(
                            schedule_state
                        )
                        if hold_reason:
                            await self._fail_recovery_hold_invariant(hold_reason)
                            return False
                        self._errors = 0
                        return True
                    if not self.snapshot.book.is_fresh(
                        self.cfg.market.max_book_age_sec
                    ):
                        raise aiohttp.ClientConnectionError(
                            "官方盘口尚未恢复到新鲜状态"
                        )
                    await self.market.refresh_context()
                    await self.broker.sync(self.market.book, force=True)  # type: ignore[call-arg]
                    self._record_fills()
                    self._record_due_markouts()
                    self._apply_dynamic_sizing()
                    self._errors = 0
                    risk_reason = self._risk_reason()
                    if risk_reason:
                        await self.halt(f"连接恢复前风险检查失败: {risk_reason}")
                        return False
                    await self._reconcile(force=True)
                    self.status = "running"
                    self._write_summary("running")
                    log.warning("官方连接、撤单和成交对账均已恢复；做市继续运行")
                    return True
                except PendingOrderReconciliation as exc:
                    self._report_pending_reconciliation()
                    log.warning("连接恢复仍在等待订单/成交终态: %s", exc)
                except Exception as exc:
                    if not self._is_transient_connectivity_error(exc):
                        await self.halt(f"连接恢复流程出现非网络错误: {exc}")
                        return False
                    log.warning("官方连接尚未完全恢复: %s", exc)
            now = time.monotonic()
            if now - last_summary >= 30.0:
                self._write_summary("paused", reason)
                last_summary = now
            await asyncio.sleep(retry_sec)
            retry_sec = min(retry_sec * 2.0, CONNECTIVITY_RETRY_MAX_SEC)
        return False

    def _risk_reason(self, *, ignore_book_staleness: bool = False) -> str | None:
        assert self.snapshot and self.broker and self.strategy
        risk = self.cfg.risk
        if self.cfg.mode == "live":
            bot_orphans = int(
                getattr(self.broker, "official_untracked_bot_orders", 0) or 0
            )
            if bot_orphans:
                return f"检测到 {bot_orphans} 笔本机器人遗留挂单未纳入当前映射"
            foreign_orders = int(
                getattr(self.broker, "official_foreign_orders", 0) or 0
            )
            if foreign_orders:
                return (
                    f"检测到 {foreign_orders} 笔同市场非本进程挂单；"
                    "为避免与外部交易并行已停机"
                )
            target = self.cfg.market.target_leverage
            if target is not None:
                kind = getattr(self.broker, "official_leverage_type", None)
                value = getattr(self.broker, "official_leverage_value", None)
                if kind is not None or value is not None:
                    if kind != "isolated" or value != target:
                        return (
                            f"官方杠杆偏离配置: {kind or 'unknown'} "
                            f"{value or 0}x，要求 isolated {target}x"
                        )
                elif abs(self.broker.account.position_qty) >= self.snapshot.meta.size_step / 2:
                    return "存在持仓但无法核验官方逐仓杠杆"
        # Book staleness, spread and oracle deviation are market states, not
        # account risk: staleness quarantines through _book_recovery_pending
        # (which only fails closed if resting orders cannot be confirmed
        # away), spread/deviation through the market-quality pause channel.
        del ignore_book_staleness
        fair = self.snapshot.fair_px
        if fair is None:
            return "无法计算 fair price"
        position_usd = abs(self.broker.account.position_qty * fair)
        if position_usd > risk.max_position_usd * 1.02:
            position_qty = self.broker.account.position_qty
            orders = list(self.broker.orders_by_role.values())
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
                + self.cfg.market_maker.order_notional_usd * 1.10
            )
            if not (
                self.cfg.strategy == "market_maker"
                and position_usd <= recovery_ceiling
                and only_reducing
            ):
                return (
                    f"持仓 ${position_usd:.2f} 超过上限 "
                    f"${risk.max_position_usd:.2f}"
                )
        liquidation_px = getattr(self.broker.account, "liquidation_px", None)
        mark = getattr(self.snapshot, "mark_px", None) or fair
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
            if distance_pct < LIQUIDATION_GUARD_MIN_DISTANCE:
                return (
                    f"强平价距离仅 {distance_pct * 100:.1f}%"
                    f"（低于 {LIQUIDATION_GUARD_MIN_DISTANCE * 100:.0f}% 保护线），"
                    "杠杆/保证金状态异常"
                )
        open_usd = sum(x.price * (x.qty - x.filled_qty) for x in self.broker.orders_by_role.values())
        if open_usd > risk.max_open_order_usd * 1.02:
            return f"挂单 ${open_usd:.2f} 超过上限 ${risk.max_open_order_usd:.2f}"
        principal_drawdown = risk.capital_usd - self.broker.account.equity_actual
        if principal_drawdown >= risk.max_principal_drawdown_usd:
            return (
                f"本金累计回撤 ${principal_drawdown:.2f} 达到停机线 "
                f"${risk.max_principal_drawdown_usd:.2f}"
            )
        if self.start_equity > 0:
            drawdown = self.start_equity - self.broker.account.equity_actual
            if drawdown >= risk.max_session_drawdown_usd:
                return f"实际权益回撤 ${drawdown:.2f} 达到停机线"
        equity = self.broker.account.equity_actual
        if risk.max_rolling_drawdown_usd > 0 and equity > 0:
            # Incident line: losing this much inside a short window is not
            # ordinary wear (a one-way move, a bug, a pickoff storm).
            now = time.monotonic()
            window_sec = risk.rolling_drawdown_window_min * 60.0
            self._equity_history.append((now, equity))
            while (
                self._equity_history
                and self._equity_history[0][0] < now - window_sec
            ):
                self._equity_history.popleft()
            burst = max(item[1] for item in self._equity_history) - equity
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
            spent = self._daily_budget_baseline(equity) - equity
            if spent >= risk.daily_loss_budget_usd:
                return (
                    f"当日亏损预算已用尽（${spent:.2f}/"
                    f"${risk.daily_loss_budget_usd:.2f}，属正常磨损成本上限；"
                    "次日自动恢复运行）"
                )
        if self.cfg.mode == "live" and risk.min_free_collateral_usd > 0:
            free = self.broker.account.withdrawable
            if free is None:
                return "无法核实可用 USDC，按 fail-closed 停机"
            if free < risk.min_free_collateral_usd:
                return f"可用 USDC ${free:.2f} 低于保留线 ${risk.min_free_collateral_usd:.2f}"
        if self._errors >= risk.max_consecutive_errors:
            return f"连续错误达到 {self._errors} 次"
        return self.strategy.halt_reason(self.snapshot, self.broker.account)

    async def _manage_aged_inventory(self) -> None:
        """Stage-3 inventory control: actively shed a stuck position.

        Passive exits (queue at the touch, then jump inside the spread) fail
        in a trend because counter-flow never arrives.  After
        INVENTORY_IOC_AFTER_SEC past the rebalance threshold, pay a small
        bounded taker cost for one ladder-half clip per minute until the
        position is back inside the threshold.
        """
        if not self.broker or not self.snapshot or self.cfg.strategy != "market_maker":
            return
        reduce_ioc = getattr(self.broker, "reduce_position_ioc", None)
        if reduce_ioc is None:
            return
        fair = self.snapshot.fair_px
        if not fair:
            return
        risk = self.cfg.risk
        ratio = (
            self.broker.account.position_qty * fair / risk.max_position_usd
            if risk.max_position_usd
            else 0.0
        )
        now = time.monotonic()
        if abs(ratio) < self.cfg.market_maker.rebalance_at_position_ratio:
            self._inventory_breach_since = None
            return
        if self._inventory_breach_since is None:
            self._inventory_breach_since = now
            return
        if now - self._inventory_breach_since < INVENTORY_IOC_AFTER_SEC:
            return
        if now - self._last_inventory_ioc < INVENTORY_IOC_INTERVAL_SEC:
            return
        self._last_inventory_ioc = now
        book = self.snapshot.book
        # order_notional_usd is the TOTAL one-side ladder budget.  Inventory
        # IOC size must not grow again merely because that budget is split
        # across more quote levels.
        step_usd = self.cfg.market_maker.order_notional_usd / 2.0
        qty = step_usd / fair
        log.warning(
            "库存被套超过 %.0f 秒（仓位占上限 %.0f%%）；主动减仓一档 $%.0f",
            now - self._inventory_breach_since,
            abs(ratio) * 100,
            step_usd,
        )
        try:
            await reduce_ioc(book, qty, INVENTORY_IOC_SLIPPAGE_BPS)
        except Exception as exc:
            log.error("主动减仓失败（保持被动退出挂单）: %s", exc)

    def _daily_budget_path(self) -> Path | None:
        ledger = getattr(self, "ledger", None)
        if ledger is None:
            return None
        return ledger.state_path.parent / "daily_loss_budget.json"

    def _daily_budget_baseline(self, equity: float) -> float:
        """Beijing-day equity baseline, persisted so restarts share it."""
        from datetime import datetime, timedelta, timezone

        today = datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d")
        if self._daily_budget_date == today:
            return self._daily_budget_start
        path = self._daily_budget_path()
        stored = read_json(path, {}, strict=False) if path else {}
        if isinstance(stored, dict) and stored.get("date") == today:
            baseline = float(stored.get("day_start_equity") or equity)
        else:
            baseline = equity
            if path is not None:
                atomic_write_json(
                    path, {"date": today, "day_start_equity": baseline}
                )
        self._daily_budget_date = today
        self._daily_budget_start = baseline
        return baseline

    async def _prepare_position_recovery(self) -> bool:
        """Cancel stale exposure-increasing quotes before the cap check.

        A fill page can become visible just after an authoritative account
        snapshot.  Until the next account refresh, applying that fill locally
        may conservatively count it twice.  The configured one-order recovery
        band is intentional, but it is safe only after old two-sided quotes
        have been removed.  This method never enlarges exposure and never
        relaxes the hard ceiling beyond that existing recovery band.
        """

        assert self.snapshot and self.broker
        if self.cfg.strategy != "market_maker":
            return False
        fair = self.snapshot.fair_px
        if fair is None:
            return False
        position_qty = self.broker.account.position_qty
        position_usd = abs(position_qty * fair)
        risk = self.cfg.risk
        if position_usd <= risk.max_position_usd * 1.02:
            return False
        recovery_ceiling = (
            risk.max_position_usd
            + self.cfg.market_maker.order_notional_usd * 1.10
        )
        if position_usd > recovery_ceiling:
            return False

        def is_reducing(order: TrackedOrder) -> bool:
            return bool(
                order.reduce_only
                and (
                    (position_qty < 0 and order.is_buy)
                    or (position_qty > 0 and not order.is_buy)
                )
            )

        stale_roles = [
            role
            for role, order in self.broker.orders_by_role.items()
            if not is_reducing(order)
        ]
        if not stale_roles:
            return False
        log.warning(
            "持仓暂时进入被动恢复带：$%.2f；先撤销 %d 张可能增加敞口的旧挂单",
            position_usd,
            len(stale_roles),
        )
        await self.broker.cancel_roles(stale_roles)
        return True

    def _needs_replace(
        self, old: TrackedOrder, new: OrderIntent, refresh_scale: float = 1.0
    ) -> bool:
        if (
            old.is_buy != new.is_buy
            or old.reduce_only != new.reduce_only
            or old.tif != new.tif
        ):
            return True
        remaining_qty = max(old.qty - old.filled_qty, 0.0)
        if self.cfg.strategy == "grid" and remaining_qty > new.qty + self.snapshot.meta.size_step / 2:
            # Grid inventory caps must never be bypassed by the MM 5% size tolerance.
            return True
        qty_drift = abs(remaining_qty - new.qty)
        qty_base = max(remaining_qty, new.qty, self.snapshot.meta.size_step)  # type: ignore[union-attr]
        # Fixed-dollar sizing changes quantity on almost every price tick.  A
        # half-step-only comparison bypassed refresh_bps and generated most of
        # the observed no-fill churn.  Only a material size change is urgent.
        if (
            qty_drift >= self.snapshot.meta.size_step / 2  # type: ignore[union-attr]
            and qty_drift / qty_base >= self.cfg.market_maker.qty_refresh_ratio
        ):
            return True
        drift_bps = abs(new.price / old.price - 1.0) * 10_000 if old.price else 1e9
        threshold = (
            self.cfg.market_maker.refresh_bps
            if self.cfg.strategy == "market_maker"
            else 0.000001
        )
        threshold *= max(refresh_scale, 1.0)
        if self.cfg.strategy == "market_maker":
            aggressive = (
                (old.is_buy and new.price > old.price)
                or (not old.is_buy and new.price < old.price)
            )
            if aggressive:
                age_sec = max(time.time() - old.created_ts, 0.0)
                if age_sec < self.cfg.market_maker.aggressive_requote_min_age_sec:
                    return False
                threshold *= self.cfg.market_maker.aggressive_requote_multiplier
        return drift_bps >= threshold

    def _budget_state(self) -> str:
        budget = getattr(self.broker, "action_budget", None)
        if bool(getattr(self.broker, "action_rate_limited", False)):
            return "hard"
        if budget is None:
            return "normal"
        return budget.state

    def _budget_pacing(self) -> tuple[float, float]:
        """Requote pacing multipliers derived from the address action budget.

        soft: slow the requote clock and widen the reprice threshold, trading
        some queue freshness for action headroom.  hard: additionally handled
        in _reconcile (no new/modified quotes; cancels stay allowed because
        cancels have a separate, larger allowance).
        """
        state = self._budget_state()
        if state == "soft":
            budget_scale = 2.0
        elif state == "hard":
            budget_scale = 4.0
        else:
            budget_scale = 1.0
        # Growth-mode volume funds only 0.1 action per dollar.  Once the local
        # sample is meaningful, require the quoting loop to approach the
        # self-funding $10/action ratio instead of merely staying below the
        # exchange's absolute cap.
        efficiency_scale = 1.0
        budget = getattr(self.broker, "action_budget", None)
        actions = float(getattr(budget, "session_spent", 0.0) or 0.0)
        if self.snapshot is not None and self.broker is not None and actions >= 100:
            target = 10.0 if self.snapshot.meta.growth_mode else 1.0
            volume_per_action = self.broker.account.volume_usd / actions
            if volume_per_action < target:
                efficiency_scale = min(
                    max(math.sqrt(target / max(volume_per_action, 0.01)), 1.0),
                    3.0,
                )
        scale = max(budget_scale, efficiency_scale)
        return scale, scale

    async def _refresh_action_budget(self) -> None:
        broker = self.broker
        if not isinstance(broker, LiveBroker):
            return
        try:
            await broker.refresh_action_budget()
        except Exception as exc:
            # Telemetry only: the exchange enforces the budget server-side,
            # and quoting decisions degrade to the last known state.
            log.warning("userRateLimit 查询失败（保留上次额度状态）: %s", str(exc)[:160])

    async def _reconcile(self, force: bool = False) -> None:
        assert self.snapshot and self.broker and self.strategy and self.ledger
        if self.grid_runtime is not None:
            self._record_fills()
            complete = await self.grid_runtime.audit(force=force)
            inventory = self.grid_runtime.snapshot()
            quantity_matches = abs(
                float(inventory.get("inventory_qty") or 0.0) - self.broker.account.position_qty
            ) < self.snapshot.meta.size_step / 2
            if not complete or not quantity_matches:
                if self.broker.orders_by_role:
                    await self.broker.cancel_roles(list(self.broker.orders_by_role))
                self.status = "paused"
                self._write_summary("paused", inventory.get("reason") or "成交推送与库存账本待对账，暂停报价")
                return
            self.status = "running"
        now = time.time()
        interval_scale, refresh_scale = self._budget_pacing()
        min_interval = (
            self.cfg.market_maker.refresh_sec
            if self.cfg.strategy == "market_maker"
            else max(self.cfg.runtime.loop_interval_sec, 0.5)
        ) * interval_scale
        if not force and now - self._last_reconcile < min_interval:
            return
        desired_list = self.strategy.desired(self.snapshot, self.broker.account)
        desired = {x.role: x for x in desired_list}
        budget_state = self._budget_state()
        cancel: list[str] = []
        modify: list[tuple[str, OrderIntent]] = []
        use_modify = self.cfg.runtime.use_batch_modify
        for role, old in self.broker.orders_by_role.items():
            new = desired.get(role)
            if new is None:
                cancel.append(role)
                continue
            if not self._needs_replace(old, new, refresh_scale):
                continue
            if (
                use_modify
                and old.tif == "Alo"
                and new.tif == "Alo"
                and old.is_buy == new.is_buy
                # A reduce-only flip is not a reprice: the exchange validates
                # the modify's new order against position/open-order state and
                # persistently rejects the flip ("Attempted to modify to
                # invalid new order"). Route semantic changes through
                # cancel+place, which was the proven path for them.
                and old.reduce_only == new.reduce_only
                and old.oid is not None
            ):
                modify.append((role, new))
            else:
                cancel.append(role)
                self.broker.requotes_replaced += 1
        if cancel:
            before_cancel = allocation_state(self.broker.account)
            await self.broker.cancel_roles(cancel)
            if self.grid_runtime is not None or allocation_state(self.broker.account) != before_cancel:
                # A cancel acknowledgement can include a late partial fill.
                # Never submit the pre-cancel allocation against new inventory.
                self._record_fills()
                self.ledger.save_state(self.strategy.state_dict(), self.broker.orders_by_role)
                self._last_reconcile = now
                return
        if budget_state == "hard":
            # Cancels above stay allowed (separate, larger cancel allowance);
            # never spend scarce actions on fresh quotes.
            log.warning("地址动作额度进入硬限流；本轮跳过改单与新挂单")
            self._last_reconcile = now
            self.ledger.save_state(
                self.strategy.state_dict(), self.broker.orders_by_role
            )
            return
        if modify:
            before_modify = allocation_state(self.broker.account)
            await self.broker.modify_orders(modify, self.snapshot.book)
            if self.grid_runtime is not None or allocation_state(self.broker.account) != before_modify:
                self._record_fills()
                self.ledger.save_state(self.strategy.state_dict(), self.broker.orders_by_role)
                self._last_reconcile = now
                return
        place = [x for x in desired_list if x.role not in self.broker.orders_by_role]
        if place:
            await self.broker.place(place, self.snapshot.book)
        if self.grid_runtime is not None:
            events = self.broker.drain_order_events()
            for event in events:
                self.grid_runtime.remember(event)
            self.ledger.record_order_events(events)
            for order in self.broker.orders_by_role.values():
                self.grid_runtime.remember(order.to_dict())
        self._last_reconcile = now
        self.ledger.save_state(self.strategy.state_dict(), self.broker.orders_by_role)

    def _record_fills(self) -> bool:
        assert self.broker and self.strategy and self.ledger and self.snapshot
        events = self.broker.drain_order_events()
        if self.grid_runtime is not None:
            for event in events:
                self.grid_runtime.remember(event)
        self.ledger.record_order_events(events)
        fills = self.broker.drain_fills()
        for fill in fills:
            if self.grid_runtime is not None:
                self.grid_runtime.remember(vars(fill))
                if self.cfg.mode == "paper":
                    self.grid_runtime.record_paper_fill(fill)
                else:
                    # Audit all official market fills in chronological order,
                    # including offline/external activity, before quoting again.
                    self.grid_runtime.last_audit = 0.0
            self.strategy.on_fill(fill)
            self.ledger.record_fill(
                fill,
                self.cfg.mode,
                self.cfg.strategy,
                self.snapshot.meta.coin,
                mid_at_record=self.snapshot.book.mid,
                account_equity_after=self.broker.account.equity_actual,
            )
            self._pending_markouts[fill.fill_id] = {
                "fill": fill,
                "remaining": {1, 5, 30},
            }
            log.info(
                "成交 %s %s %.8f @ %.8f $%.2f fee=$%.6f projected_rebate=$%.6f",
                fill.order_role,
                "BUY" if fill.is_buy else "SELL",
                fill.qty,
                fill.price,
                fill.notional,
                fill.fee_usd,
                fill.projected_self_rebate + fill.projected_referred_benefit,
            )
        return bool(fills)

    def _record_due_markouts(self) -> None:
        assert self.ledger and self.snapshot
        if not self._mid_history:
            return
        now_ms = int(time.time() * 1000)
        for fill_id, item in list(self._pending_markouts.items()):
            fill = item["fill"]
            remaining = item["remaining"]
            for horizon in sorted(list(remaining)):
                target_ms = fill.time_ms + horizon * 1000
                if now_ms < target_ms:
                    continue
                sample = next(
                    (
                        (sample_ms, sample_mid)
                        for sample_ms, sample_mid in self._mid_history
                        if sample_ms >= target_ms
                    ),
                    None,
                )
                if sample is None:
                    continue
                sample_ms, sample_mid = sample
                markout = self.ledger.record_markout(
                    fill,
                    horizon,
                    sample_mid,
                    observed_ms=sample_ms,
                )
                if (
                    isinstance(markout, dict)
                    and self.strategy is not None
                    and self.strategy.on_markout(markout)
                ):
                    # Reprice promptly, while still passing through the normal
                    # post-only replacement and risk gates in the main loop.
                    self._last_reconcile = 0.0
                remaining.remove(horizon)
            if not remaining:
                self._pending_markouts.pop(fill_id, None)

    def _write_summary(self, status: str, reason: str | None = None) -> None:
        if not all((self.ledger, self.broker, self.snapshot, self.fees)):
            return
        order_details = []
        for order in sorted(
            self.broker.orders_by_role.values(), key=lambda item: item.created_ts
        ):
            remaining_qty = max(order.qty - order.filled_qty, 0.0)
            order_details.append(
                {
                    "order_role": order.role,
                    "oid": order.oid,
                    "cloid": order.cloid,
                    "is_buy": order.is_buy,
                    "side": "BUY" if order.is_buy else "SELL",
                    "price": order.price,
                    "remaining_qty": remaining_qty,
                    "original_qty": order.qty,
                    "filled_qty": order.filled_qty,
                    "notional_usd": order.price * remaining_qty,
                    "timestamp_ms": int(order.created_ts * 1000),
                    "reduce_only": order.reduce_only,
                    "order_type": "Limit",
                    "tif": order.tif,
                    "source": "bot_reconciled",
                }
            )
        self.ledger.write_summary(
            status=status,
            reason=reason,
            account=self.broker.account,
            market=self.snapshot,
            fees=self.fees,
            open_orders=len(self.broker.orders_by_role),
            session_start_equity=self.start_equity,
            session_started_at_ms=self.session_started_at_ms,
            configured_capital_usd=self.cfg.risk.capital_usd,
            session_start_market_unrealized=self.start_market_unrealized,
            open_order_details=order_details,
            target_leverage=self.cfg.market.target_leverage,
            wallet_id=self.wallet_id,
            risk_profile=effective_risk_profile(self.cfg),
            adaptive_spread_extra_bps=float(
                getattr(self.strategy, "adaptive_extra_bps", 0.0)
            ),
            adaptive_spread_metrics=dict(
                getattr(self.strategy, "adaptive_metrics", {}) or {}
            ),
            action_budget=(
                self.broker.action_budget.snapshot(
                    session_volume_usd=self.broker.account.volume_usd,
                    growth_mode=self.snapshot.meta.growth_mode,
                )
                if isinstance(self.broker, LiveBroker)
                else None
            ),
            ws_feeds={
                "user_feeds_enabled": bool(
                    self.cfg.runtime.use_ws_user_feeds
                    and self.cfg.mode == "live"
                ),
                "healthy": bool(self.broker.ws_healthy()),
                "session": int(getattr(self.market, "ws_session", 0) or 0),
            },
            requote_stats={
                "modified": int(self.broker.requotes_modified),
                "replaced": int(self.broker.requotes_replaced),
            },
            grid_accounting=(
                self.grid_runtime.snapshot(self.snapshot.mark_px or self.snapshot.fair_px)
                if self.grid_runtime is not None else None
            ),
        )
    def _publish_summary(self) -> None:
        now = time.time()
        if now - self._last_summary < self.cfg.runtime.summary_refresh_sec:
            return
        self._last_summary = now
        self._write_summary(
            self.status,
            self._schedule_reason
            or self._market_quality_reason
            or self._book_stale_reason,
        )

    def _heartbeat(self) -> None:
        assert self.snapshot and self.broker
        now = time.time()
        if now - self._last_heartbeat < self.cfg.runtime.heartbeat_sec:
            return
        self._last_heartbeat = now
        a = self.broker.account
        log.info(
            "%s心跳 fair=%.6f pos=%.8f equity=$%.4f projected=$%.4f orders=%d volume=$%.2f",
            "暂停" if self._schedule_paused or self._market_quality_paused else "",
            self.snapshot.fair_px or 0.0,
            a.position_qty,
            a.equity_actual,
            a.equity_after_projected_rebate,
            len(self.broker.orders_by_role),
            a.volume_usd,
        )
        self._write_summary(
            self.status,
            self._schedule_reason
            or self._market_quality_reason
            or self._book_stale_reason,
        )

    def _report_pending_reconciliation(self) -> None:
        """Keep fills and local health telemetry current while placement waits.

        A broker sync can safely raise PendingOrderReconciliation for several
        minutes.  Any fill fragments recovered before that exception are still
        authoritative and must reach the ledger immediately; otherwise the
        dashboard appears stale even though the process and safety guard are
        making progress.  Heartbeat/summary methods retain their normal rate
        limits, so calling this on each pending loop does not create write spam.
        """

        self._record_fills()
        self._record_due_markouts()
        self._heartbeat()
        self._publish_summary()

    async def _cancel_and_reconcile_exit(
        self,
        context: str,
        *,
        timeout_sec: float = 305.0,
        sleep_sec: float = 0.5,
    ) -> bool:
        """Cancel once, then keep official fill/order reconciliation alive.

        A UI stop can arrive while orderStatus already says ``filled`` but
        userFills is still delayed.  Exiting immediately would preserve order
        safety but lose that fill from the local wear ledger.  The broker's
        cloid guards make repeated calls idempotent: no cancel is resubmitted,
        while official state is polled until the mapping reaches a terminal
        state or the bounded reconciliation window expires.
        """
        if not self.broker:
            return True
        deadline = time.monotonic() + timeout_sec
        last_notice = 0.0
        while True:
            try:
                if self.cfg.mode == "live" and self.snapshot:
                    await self.broker.sync(self.snapshot.book, force=True)  # type: ignore[call-arg]
                    if self.ledger and self.strategy:
                        self._record_fills()
                        self._record_due_markouts()
                await self.broker.cancel_all()
                if self.cfg.mode == "live" and hasattr(
                    self.broker, "cancel_bot_orphans"
                ):
                    orphan_count = await self.broker.cancel_bot_orphans()  # type: ignore[attr-defined]
                    if orphan_count:
                        log.warning("%s撤销了 %d 笔本机器人遗留订单", context, orphan_count)
                    foreign_orders = int(
                        getattr(self.broker, "official_foreign_orders", 0) or 0
                    )
                    if foreign_orders:
                        log.error(
                            "%s发现 %d 笔同市场非本进程挂单；未自动撤销",
                            context,
                            foreign_orders,
                        )
                        return False
                if self.cfg.mode == "live" and self.snapshot:
                    await self.broker.sync(self.snapshot.book, force=True)  # type: ignore[call-arg]
                    if self.ledger and self.strategy:
                        self._record_fills()
                        self._record_due_markouts()
                if not self.broker.orders_by_role:
                    return True
            except PendingOrderReconciliation as exc:
                now = time.monotonic()
                if now >= deadline:
                    log.error("%s对账超过 %.0f 秒: %s", context, timeout_sec, exc)
                    return False
                if now - last_notice >= 5.0:
                    log.warning("%s等待订单/成交终态: %s", context, exc)
                    last_notice = now
            except Exception as exc:
                log.error("%s撤单未完全确认: %s", context, exc)
                return False
            if time.monotonic() >= deadline:
                log.error("%s对账超过 %.0f 秒，保留订单映射后退出", context, timeout_sec)
                return False
            await asyncio.sleep(sleep_sec)

    async def _finalize_exit_order_state(self) -> None:
        """Prove ordinary exit is clean or promote it to a fail-closed halt."""

        if (
            not self.broker
            or not self.cfg.runtime.cancel_strategy_orders_on_exit
            or self.status == "halted"
        ):
            return
        try:
            orders_clean = await self._cancel_and_reconcile_exit("退出")
        except Exception as exc:
            orders_clean = False
            reason = (
                f"退出撤单/订单终态对账异常（{type(exc).__name__}）；"
                "未确认官方挂单已清空，需要立即核对官方挂单"
            )
            log.error("%s: %s", reason, exc)
        else:
            reason = (
                "退出撤单或订单终态未确认干净；"
                "可能仍有本机器人或同市场外部挂单，"
                "需要立即核对官方挂单"
            )
        if not orders_clean:
            self.status = "halted"
            self.halt_reason_text = reason

    async def halt(self, reason: str) -> None:
        if self.status == "halted":
            return
        self.status = "halted"
        self.halt_reason_text = reason
        log.error("风险停机: %s", reason)
        if self.broker:
            try:
                orders_clean = await self._cancel_and_reconcile_exit("停机")
            except Exception as exc:
                orders_clean = False
                log.error("停机撤单/订单终态对账异常: %s", exc)
            if not orders_clean:
                reason = (
                    f"{reason}；警告：停机撤单或订单终态未确认干净，"
                    "可能仍有本机器人或同市场外部挂单；需要立即核对官方挂单"
                )
                self.halt_reason_text = reason
            if self.cfg.risk.flatten_on_halt and self.snapshot:
                # The stop-loss is only closed-loop if the flatten actually
                # happens.  A halt often coincides with the exact conditions
                # that make one IOC attempt fail (connectivity blip, stale
                # book, price ran through the slippage cap), so retry with a
                # fresh book instead of leaving inventory unmanaged.
                flattened = False
                flatten_attempted = False
                flatten_uncertain = False
                position_before_flatten = abs(
                    float(
                        getattr(
                            getattr(self.broker, "account", None),
                            "position_qty",
                            0.0,
                        )
                    )
                )
                for attempt in range(1, FLATTEN_RETRY_ATTEMPTS + 1):
                    try:
                        flatten_attempted = True
                        book = self.snapshot.book
                        if attempt > 1 and self.market is not None:
                            try:
                                book = await self.market.rest_book_once()
                            except Exception as exc:
                                log.warning("平仓重试前刷新盘口失败: %s", exc)
                        await self.broker.flatten(
                            book, self.cfg.risk.emergency_slippage_bps
                        )
                        flattened = True
                        break
                    except UncertainIocOutcome as exc:
                        # A new cloid here could duplicate an action whose
                        # acceptance is still unknown.  Stop the retry loop;
                        # LiveBroker retains that cloid and the order ledger
                        # records it for stopped-process reconciliation.
                        flatten_uncertain = True
                        log.error("自动平仓结果不确定，禁止盲目重发: %s", exc)
                        break
                    except Exception as exc:
                        log.error(
                            "自动减仓尝试 %d/%d 失败: %s",
                            attempt,
                            FLATTEN_RETRY_ATTEMPTS,
                            exc,
                        )
                        if attempt < FLATTEN_RETRY_ATTEMPTS:
                            await asyncio.sleep(FLATTEN_RETRY_DELAY_SEC * attempt)
                # Pull official views one last time while IOC oid/cloid
                # mappings are alive, then drain both identity/action events
                # and any fill details before the final summary is written.
                recorded_flatten_fill = False
                final_sync_error: Exception | None = None
                if flatten_attempted and isinstance(self.broker, LiveBroker):
                    try:
                        await self.broker.sync(self.snapshot.book, force=True)
                    except Exception as exc:
                        final_sync_error = exc
                        log.warning("停机前紧急平仓明细最终同步失败: %s", exc)
                if flatten_attempted and self.ledger and self.strategy:
                    try:
                        recorded_flatten_fill = self._record_fills()
                        self._record_due_markouts()
                    except Exception as exc:
                        final_sync_error = final_sync_error or exc
                        log.warning("停机前紧急平仓台账记录失败: %s", exc)

                if flatten_uncertain:
                    reason = (
                        f"{reason}；警告：自动平仓提交结果不确定，"
                        "已保留 cloid 对账身份（若已获取 oid 也已登记）"
                        "并禁止重发，"
                        "需要立即核对官方仓位"
                    )
                    self.halt_reason_text = reason
                elif not flattened:
                    reason = (
                        f"{reason}；警告：自动平仓 {FLATTEN_RETRY_ATTEMPTS} 次"
                        "均失败，可能存在未平仓位，需要立即人工处理"
                    )
                    self.halt_reason_text = reason
                    log.error("自动减仓最终失败，需要人工处理")
                elif position_before_flatten >= 1e-12 and (
                    not recorded_flatten_fill or final_sync_error is not None
                ):
                    reason = (
                        f"{reason}；提示：自动平仓仓位已确认归零，"
                        "但成交明细仍可能延迟；IOC cloid/oid 身份已写入"
                        "订单台账，下次启动前需执行停机对账"
                    )
                    self.halt_reason_text = reason
                    log.warning("平仓仓位已归零，但成交明细尚未完整入账")
        self._write_summary("halted", reason)
        self.stop.set()

    async def run(self, duration_sec: float | None = None) -> None:
        try:
            await self.initialize()
        except BaseException:
            if self.session and not self.session.closed:
                await self.session.close()
            if self.grid_runtime is not None:
                self.grid_runtime.close()
            if self._instance_lock is not None:
                self._instance_lock.close()
            raise
        assert self.market and self.snapshot and self.broker
        # The REST snapshot used during initialization must not satisfy the
        # websocket readiness gate. Clear it before scheduling the feed so the
        # first reconciliation always uses a confirmed streaming snapshot.
        self.market.book.clear()
        ws_user_address = None
        ws_user_sink = None
        if (
            isinstance(self.broker, LiveBroker)
            and self.cfg.runtime.use_ws_user_feeds
        ):
            ws_user_address = self.broker.address
            ws_user_sink = self.broker.ingest_ws_event
        task = asyncio.create_task(
            self.market.run_feeds(
                self.stop,
                self.notify_book,
                user_address=ws_user_address,
                on_user_event=ws_user_sink,
            ),
            name="entropy-feeds",
        )
        started = time.time()
        try:
            # Allow the websocket snapshot to arrive before treating staleness as fatal.
            deadline = time.time() + 15
            while not self.market.book.ready and time.time() < deadline:
                external_reason = self._external_stop_reason()
                if external_reason:
                    log.warning("%s；安全撤单退出", external_reason)
                    return
                await asyncio.sleep(0.1)
            if not self.market.book.ready:
                await self.halt("15 秒内未收到官方盘口")
                return
            self._ever_had_book = True
            await self.market.refresh_context()
            await self.broker.sync(self.market.book)
            self._apply_dynamic_sizing()
            self.start_equity = self.broker.account.equity_actual
            self.start_market_unrealized = self.broker.account.unrealized_pnl
            initial_risk_reason = self._risk_reason()
            if initial_risk_reason:
                await self.halt(f"启动前风险检查失败: {initial_risk_reason}")
                return
            schedule_state = self._effective_trading_schedule_state()
            if schedule_state.paused:
                await self._enter_schedule_pause(schedule_state)
                hold_reason = self._recovery_hold_invariant_reason(schedule_state)
                if hold_reason:
                    await self._fail_recovery_hold_invariant(hold_reason)
                    return
            else:
                await self._reconcile(force=True)

            while not self.stop.is_set():
                if duration_sec is not None and time.time() - started >= duration_sec:
                    break
                external_reason = self._external_stop_reason()
                if external_reason:
                    log.warning("%s；安全撤单退出", external_reason)
                    break
                try:
                    now = time.time()
                    schedule_state = self._effective_trading_schedule_state()
                    if schedule_state.paused:
                        await self._enter_schedule_pause(schedule_state)
                        hold_reason = self._recovery_hold_invariant_reason(
                            schedule_state
                        )
                        if hold_reason:
                            await self._fail_recovery_hold_invariant(hold_reason)
                            break
                    elif self._schedule_paused:
                        if not await self._resume_after_schedule_pause():
                            break

                    context_interval = (
                        max(self.cfg.market.context_refresh_sec, 60.0)
                        if self._schedule_paused
                        else self.cfg.market.context_refresh_sec
                    )
                    if now - self._last_context >= context_interval:
                        await self.market.refresh_context()
                        self._last_context = now
                    sync_interval = (
                        self.cfg.runtime.loop_interval_sec
                        if self.cfg.mode == "paper"
                        else max(
                            self.cfg.runtime.account_refresh_sec,
                            self.cfg.runtime.fill_refresh_sec,
                            60.0,
                        )
                        if self._schedule_paused
                        else min(
                            self.cfg.runtime.account_refresh_sec,
                            self.cfg.runtime.fill_refresh_sec,
                        )
                    )
                    if (
                        self.cfg.mode == "live"
                        and not self._schedule_paused
                        and self.broker.ws_healthy()
                    ):
                        # Push state makes each sync cheap; run it every loop
                        # so fills re-arm quotes within one tick.
                        sync_interval = min(sync_interval, 1.0)
                    if now - self._last_sync >= sync_interval:
                        await self.broker.sync(self.market.book)
                        self._apply_dynamic_sizing()
                        self._last_sync = now
                        if not self._schedule_paused:
                            await self._refresh_action_budget()
                        if self._schedule_paused:
                            hold_reason = self._recovery_hold_invariant_reason(
                                schedule_state
                            )
                            if hold_reason:
                                await self._fail_recovery_hold_invariant(hold_reason)
                                break
                    had_fills = self._record_fills()
                    self._record_due_markouts()
                    book_recovery_pending = False
                    if not self._schedule_paused:
                        book_recovery_pending = await self._book_recovery_pending()
                    if book_recovery_pending:
                        self._heartbeat()
                        self._publish_summary()
                        self._errors = 0
                    else:
                        self._track_oracle_liveness()
                        market_pause = (
                            None
                            if self._schedule_paused
                            else self._market_quality_pause_reason()
                        )
                        crisis_active = False
                        if market_pause and self._crisis_quotes_allowed():
                            # Dislocated book with a live oracle anchor:
                            # quote the far touch instead of standing aside.
                            crisis_active = True
                            market_pause = None
                        if self.strategy is not None:
                            was_crisis = getattr(
                                self.strategy, "crisis_mode", False
                            )
                            if crisis_active != was_crisis:
                                log.warning(
                                    "危机报价模式 %s（盘口异常 + oracle 锚%s）",
                                    "进入" if crisis_active else "退出",
                                    "存活" if crisis_active else "状态变化",
                                )
                            self.strategy.crisis_mode = crisis_active
                        if crisis_active or market_pause:
                            self._note_market_quality_snapshot(
                                crisis=crisis_active
                            )
                        if market_pause:
                            await self._enter_market_quality_pause(market_pause)
                            self._heartbeat()
                            self._publish_summary()
                            self._errors = 0
                        else:
                            if self._market_quality_paused:
                                if not await self._resume_after_market_quality_pause():
                                    break
                            if not self._schedule_paused:
                                await self._prepare_position_recovery()
                            reason = self._risk_reason(
                                ignore_book_staleness=self._schedule_paused
                            )
                            if reason:
                                await self.halt(reason)
                                break
                            if not self._schedule_paused:
                                await self._manage_aged_inventory()
                                await self._reconcile(force=had_fills)
                            self._heartbeat()
                            self._publish_summary()
                            self._errors = 0
                except asyncio.CancelledError:
                    raise
                except PendingOrderReconciliation as exc:
                    # Keep the cloid/oid mapping reserved. A later official
                    # order/fill view can then reconcile without placing a
                    # duplicate order under the same strategy role.
                    self._errors = 0
                    self._report_pending_reconciliation()
                    log.warning("订单状态等待对账: %s", exc)
                except Exception as exc:
                    self._errors += 1
                    log.exception("主循环错误 (%d/%d): %s", self._errors, self.cfg.risk.max_consecutive_errors, exc)
                    if self._errors >= self.cfg.risk.max_consecutive_errors:
                        if self._is_transient_connectivity_error(exc):
                            if not await self._recover_transient_connectivity(exc):
                                break
                        else:
                            await self.halt(f"连续主循环错误: {exc}")
                            break
                self.book_event.clear()
                try:
                    await asyncio.wait_for(
                        self.book_event.wait(), timeout=self.cfg.runtime.loop_interval_sec
                    )
                except asyncio.TimeoutError:
                    pass
        finally:
            self.stop.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await self._finalize_exit_order_state()
            if self.grid_runtime is not None:
                await self.grid_runtime.audit(force=True)
            if self.ledger and self.strategy and self.broker:
                self.ledger.save_state(
                    self.strategy.state_dict(), self.broker.orders_by_role
                )
            if self.ledger and self.snapshot:
                self._record_due_markouts()
            self._write_summary(
                "halted" if self.status == "halted" else "stopped",
                self.halt_reason_text,
            )
            if self.session:
                await self.session.close()
            if self.grid_runtime is not None:
                # Shutdown can race final fills; save a final read-only view.
                self.grid_runtime.close()
            if self._instance_lock is not None:
                self._instance_lock.close()


async def check_config_live_data(cfg: Config) -> dict:
    async with aiohttp.ClientSession(trust_env=False) as session:
        market = MarketClient(cfg.market, session)
        meta = await market.load_meta()
        await asyncio.gather(market.refresh_context(), market.rest_book_once())
        credentials = resolve_live_credentials(cfg, require_private_key=False)
        address = credentials.account_address or ""
        user_fees = None
        fee_source = "conservative_base"
        wallet_state = None
        if address.startswith("0x") and len(address) == 42:
            (
                user_fees,
                account_state,
                account_role,
                abstraction,
                dex_abstraction,
                spot_state,
            ) = await asyncio.gather(
                market.user_fees(address),
                market.account_state(address),
                market.user_role(address),
                market.user_abstraction(address),
                market.user_dex_abstraction(address),
                market.spot_state(address),
            )
            fee_source = "account_userFees"
            summary = account_state.get("marginSummary") or {}
            spot_usdc = next(
                (
                    item
                    for item in spot_state.get("balances") or []
                    if item.get("coin") == "USDC"
                ),
                {},
            )
            wallet_state = {
                "role": account_role.get("role"),
                "account_abstraction": abstraction,
                "io_account_value": summary.get("accountValue"),
                "io_withdrawable": account_state.get("withdrawable"),
                "io_positions": len(account_state.get("assetPositions") or []),
                "spot_usdc_total": spot_usdc.get("total"),
                "spot_usdc_hold": spot_usdc.get("hold"),
                "legacy_dex_abstraction": dex_abstraction,
                "address_source": credentials.source,
                "address_derived_from_private_key": credentials.account_was_derived,
            }
        fees = build_fee_model(
            meta,
            user_fees,
            self_rebate_pct=cfg.fees.entropy_self_rebate_pct,
            referred_benefit_pct=cfg.fees.entropy_referred_user_benefit_pct,
        )
        snapshot = market.snapshot
        assert snapshot is not None
        return {
            "ok": True,
            "mode": cfg.mode,
            "strategy": cfg.strategy,
            "coin": meta.coin,
            "is_delisted": meta.is_delisted,
            "growth_mode": meta.growth_mode,
            "max_leverage": meta.max_leverage,
            "isolated_only": meta.only_isolated,
            "bid": snapshot.book.best_bid,
            "ask": snapshot.book.best_ask,
            "spread_bps": snapshot.book.spread_bps,
            "mark_px": snapshot.mark_px,
            "oracle_px": snapshot.oracle_px,
            "day_notional_volume": snapshot.day_notional_volume,
            "fee_source": fee_source,
            "wallet_state": wallet_state,
            "fee_model": fees.to_dict(),
            "maker_round_trip_projected_cost_bps": fees.projected_net_maker_rate * 20_000,
            "configured_grid_spacing_bps": cfg.grid.spacing_bps,
        }
