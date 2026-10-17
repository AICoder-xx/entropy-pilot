import asyncio
import json
import time
import aiohttp
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from entropy_bot.config import load_config
from entropy_bot.broker import (
    LiveBroker,
    PendingFillReconciliation,
    UncertainIocOutcome,
)
from entropy_bot.engine import Engine, RECOVERY_HOLD_REASON
from entropy_bot.models import (
    AccountState,
    FeeModel,
    Fill,
    MarketMeta,
    MarketSnapshot,
    OrderBook,
    TrackedOrder,
)
from entropy_bot.trading_schedule import TradingScheduleState


def test_ui_control_heartbeat_and_stop(tmp_path):
    cfg = load_config(Path(__file__).parents[1] / "config.market_maker.json")
    control = tmp_path / "control.json"
    run_id = "test-run"
    engine = Engine(cfg, control_file=control, run_id=run_id)

    control.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "desired_state": "running",
                "dashboard_heartbeat_ms": int(time.time() * 1000),
            }
        ),
        encoding="utf-8",
    )
    assert engine._external_stop_reason() is None

    control.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "desired_state": "stopped",
                "dashboard_heartbeat_ms": int(time.time() * 1000),
            }
        ),
        encoding="utf-8",
    )
    assert engine._external_stop_reason() == "UI 已请求停止"


def test_dashboard_run_id_is_the_exact_ledger_session_id():
    cfg = load_config(Path(__file__).parents[1] / "config.market_maker.json")
    engine = Engine(cfg, run_id="dashboard-run-identity")

    assert engine.session_id == "dashboard-run-identity"


def test_dynamic_sizing_reloads_strategy_state_through_regime_guard():
    base = load_config(Path(__file__).parents[1] / "config.market_maker.json")
    cfg = replace(
        base,
        risk=replace(
            base.risk,
            auto_scale_with_equity=True,
            auto_scale_max_equity_usd=base.risk.capital_usd + 1000.0,
            auto_scale_step_usd=100.0,
        ),
    )
    engine = Engine(cfg)
    engine.broker = SimpleNamespace(  # type: ignore[assignment]
        account=SimpleNamespace(equity_actual=base.risk.capital_usd + 500.0),
        cfg=cfg,
    )

    class Strategy:
        def __init__(self):
            self.cfg = cfg
            self.loaded = []

        def state_dict(self):
            return {"adaptive_regime_key": "old", "adaptive_extra_bps": 1.0}

        def load_state(self, state):
            self.loaded.append((state, self.cfg.risk.capital_usd))

    strategy = Strategy()
    engine.strategy = strategy  # type: ignore[assignment]

    assert engine._apply_dynamic_sizing() is True
    assert strategy.loaded == [
        (
            {"adaptive_regime_key": "old", "adaptive_extra_bps": 1.0},
            base.risk.capital_usd + 500.0,
        )
    ]
    assert engine.broker.cfg is engine.cfg


def test_pending_reconciliation_reports_recovered_fills_and_health():
    cfg = load_config(Path(__file__).parents[1] / "config.market_maker.json")
    engine = Engine(cfg)
    calls: list[str] = []
    engine._record_fills = lambda: calls.append("fills")  # type: ignore[method-assign]
    engine._record_due_markouts = lambda: calls.append("markouts")  # type: ignore[method-assign]
    engine._heartbeat = lambda: calls.append("heartbeat")  # type: ignore[method-assign]
    engine._publish_summary = lambda: calls.append("summary")  # type: ignore[method-assign]

    engine._report_pending_reconciliation()

    assert calls == ["fills", "markouts", "heartbeat", "summary"]


def test_exit_waits_for_delayed_fill_reconciliation_before_returning():
    cfg = replace(
        load_config(Path(__file__).parents[1] / "config.market_maker.json"),
        mode="live",
    )
    engine = Engine(cfg)
    engine.snapshot = SimpleNamespace(book=OrderBook())

    class Broker:
        def __init__(self):
            self.orders_by_role = {"mm:bid:0": object()}
            self.cancel_calls = 0
            self.sync_calls = 0
            self.orphan_calls = 0
            self.official_foreign_orders = 0

        async def sync(self, _book, *, force=False):
            assert force is True
            self.sync_calls += 1

        async def cancel_all(self):
            self.cancel_calls += 1
            if self.cancel_calls == 1:
                raise PendingFillReconciliation("成交明细延迟")
            self.orders_by_role.clear()

        async def cancel_bot_orphans(self):
            self.orphan_calls += 1
            return 0

    broker = Broker()
    engine.broker = broker  # type: ignore[assignment]

    result = asyncio.run(
        engine._cancel_and_reconcile_exit(
            "测试退出", timeout_sec=1.0, sleep_sec=0.0
        )
    )

    assert result is True
    assert broker.cancel_calls == 2
    assert broker.sync_calls >= 2
    assert broker.orphan_calls == 1


def test_schedule_pause_rescans_official_orders_every_minute(monkeypatch):
    cfg = replace(
        load_config(Path(__file__).parents[1] / "config.market_maker.json"),
        mode="live",
    )
    engine = Engine(cfg)
    engine.broker = SimpleNamespace(orders_by_role={})  # type: ignore[assignment]
    calls = []

    async def fake_cancel(context):
        calls.append(context)
        return True

    engine._cancel_and_reconcile_exit = fake_cancel  # type: ignore[method-assign]
    now = [100.0]
    monkeypatch.setattr("entropy_bot.engine.time.monotonic", lambda: now[0])
    state = SimpleNamespace(reason="交易时段保护")

    asyncio.run(engine._enter_schedule_pause(state))
    now[0] = 159.0
    asyncio.run(engine._enter_schedule_pause(state))
    now[0] = 160.0
    asyncio.run(engine._enter_schedule_pause(state))

    assert calls == ["交易时段暂停", "交易时段暂停"]


def test_recovery_hold_stays_paused_until_real_session_pause_then_releases(
    monkeypatch, tmp_path
):
    cfg = load_config(Path(__file__).parents[1] / "config.market_maker.json")
    control = tmp_path / "control.json"
    run_id = "recovery-hold-run"
    control.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "desired_state": "running",
                "recovery_hold_until_session_pause": True,
            }
        ),
        encoding="utf-8",
    )
    engine = Engine(cfg, control_file=control, run_id=run_id)
    now = datetime(2026, 8, 29, 15, 0, tzinfo=timezone.utc)
    states = [
        TradingScheduleState(False, None, now),
        TradingScheduleState(True, "真实交易时段保护", now),
        TradingScheduleState(False, None, now),
    ]
    monkeypatch.setattr(engine, "_trading_schedule_state", lambda: states.pop(0))

    held = engine._effective_trading_schedule_state()
    assert held.paused is True
    assert "恢复隔离" in str(held.reason)
    assert json.loads(control.read_text(encoding="utf-8"))[
        "recovery_hold_until_session_pause"
    ] is True

    actual_pause = engine._effective_trading_schedule_state()
    assert actual_pause.reason == "真实交易时段保护"
    released = json.loads(control.read_text(encoding="utf-8"))
    assert released["recovery_hold_until_session_pause"] is False
    assert released["recovery_hold_released_at_ms"] > 0

    resumed = engine._effective_trading_schedule_state()
    assert resumed.paused is False


def test_recovery_hold_requires_zero_orders_and_position():
    cfg = load_config(Path(__file__).parents[1] / "config.market_maker.json")
    engine = Engine(cfg)
    engine.snapshot = SimpleNamespace(
        meta=SimpleNamespace(size_step=0.0001)
    )  # type: ignore[assignment]
    engine.broker = SimpleNamespace(
        account=SimpleNamespace(position_qty=0.0),
        orders_by_role={},
    )  # type: ignore[assignment]
    held = TradingScheduleState(
        True,
        RECOVERY_HOLD_REASON,
        datetime(2026, 8, 29, 15, 0, tzinfo=timezone.utc),
    )

    assert engine._recovery_hold_invariant_reason(held) is None

    engine.broker.account.position_qty = 0.000049
    assert engine._recovery_hold_invariant_reason(held) is None

    engine.broker.account.position_qty = 0.00005
    assert "持仓" in str(engine._recovery_hold_invariant_reason(held))

    engine.broker.account.position_qty = 0.0
    engine.broker.orders_by_role = {"mm:bid:0": object()}
    assert "挂单" in str(engine._recovery_hold_invariant_reason(held))

    ordinary_pause = replace(held, reason="真实交易时段保护")
    assert engine._recovery_hold_invariant_reason(ordinary_pause) is None


def test_recovery_hold_failure_never_flattens():
    cfg = load_config(Path(__file__).parents[1] / "config.market_maker.json")
    engine = Engine(cfg)
    calls: list[str] = []
    engine.broker = SimpleNamespace(
        flatten=lambda *_args, **_kwargs: calls.append("flatten")
    )  # type: ignore[assignment]
    engine._write_summary = (  # type: ignore[method-assign]
        lambda status, reason=None: calls.append(f"summary:{status}:{reason}")
    )

    asyncio.run(engine._fail_recovery_hold_invariant("恢复隔离要求本地持仓为 0"))

    assert engine.status == "halted"
    assert engine.stop.is_set()
    assert "flatten" not in calls
    assert calls == ["summary:halted:恢复隔离检查失败: 恢复隔离要求本地持仓为 0"]


def test_live_risk_gate_rejects_unowned_orders_and_leverage_drift():
    base = load_config(Path(__file__).parents[1] / "config.market_maker.json")
    cfg = replace(
        base,
        mode="live",
        market=replace(base.market, target_leverage=3),
    )
    engine = Engine(cfg)
    meta = MarketMeta(
        coin="io:SNDK",
        dex="io",
        symbol="SNDK",
        sz_decimals=4,
        max_leverage=10,
        only_isolated=True,
        growth_mode=True,
        deployer_fee_scale=1,
        is_delisted=False,
        dex_index=1,
        universe_index=2,
        asset_id=110002,
    )
    book = OrderBook()
    book.apply_hl(
        [[{"px": "99.99", "sz": "10"}], [{"px": "100.01", "sz": "10"}]]
    )
    engine.snapshot = MarketSnapshot(meta, book, mark_px=100.0, oracle_px=100.0)
    broker = SimpleNamespace(
        account=AccountState(
            equity_actual=cfg.risk.capital_usd,
            withdrawable=cfg.risk.capital_usd,
        ),
        orders_by_role={},
        official_untracked_bot_orders=0,
        official_foreign_orders=1,
        official_leverage_type="isolated",
        official_leverage_value=3,
    )
    engine.broker = broker  # type: ignore[assignment]
    engine.strategy = SimpleNamespace(halt_reason=lambda _market, _account: None)
    engine.start_equity = cfg.risk.capital_usd

    assert "非本进程挂单" in engine._risk_reason()

    broker.official_foreign_orders = 0
    broker.official_untracked_bot_orders = 1
    assert "遗留挂单" in engine._risk_reason()

    broker.official_untracked_bot_orders = 0
    broker.official_leverage_value = 10
    assert "杠杆偏离" in engine._risk_reason()

    broker.official_leverage_value = 3
    assert engine._risk_reason() is None


def test_small_position_overshoot_is_allowed_only_with_reduce_only_recovery():
    cfg = load_config(Path(__file__).parents[1] / "config.market_maker.json")
    engine = Engine(cfg)
    meta = MarketMeta(
        coin="io:SNDK",
        dex="io",
        symbol="SNDK",
        sz_decimals=4,
        max_leverage=10,
        only_isolated=True,
        growth_mode=True,
        deployer_fee_scale=1,
        is_delisted=False,
        dex_index=1,
        universe_index=2,
        asset_id=110002,
    )
    book = OrderBook()
    book.apply_hl(
        [[{"px": "99.99", "sz": "10"}], [{"px": "100.01", "sz": "10"}]]
    )
    engine.snapshot = MarketSnapshot(meta, book, mark_px=100.0, oracle_px=100.0)
    position_qty = -(cfg.risk.max_position_usd * 1.03) / 100.0
    account = AccountState(
        position_qty=position_qty,
        equity_actual=1000.0,
        withdrawable=1000.0,
    )
    recovery = TrackedOrder(
        role="mm:bid:0",
        cloid="0x" + "5" * 32,
        is_buy=True,
        price=99.99,
        qty=cfg.market_maker.order_notional_usd / 99.99,
        reduce_only=True,
        tif="Alo",
        created_ts=0.0,
    )
    engine.broker = SimpleNamespace(
        account=account, orders_by_role={recovery.role: recovery}
    )
    engine.strategy = SimpleNamespace(halt_reason=lambda _market, _account: None)
    engine.start_equity = 1000.0

    assert engine._risk_reason() is None

    engine.broker.orders_by_role = {}
    assert engine._risk_reason() is None

    engine.broker.orders_by_role = {recovery.role: recovery}

    recovery.reduce_only = False
    assert "超过上限" in engine._risk_reason()


def test_position_recovery_cancels_stale_quotes_before_risk_gate():
    cfg = load_config(Path(__file__).parents[1] / "config.market_maker.json")
    engine = Engine(cfg)
    meta = MarketMeta(
        coin="io:SNDK",
        dex="io",
        symbol="SNDK",
        sz_decimals=4,
        max_leverage=10,
        only_isolated=True,
        growth_mode=True,
        deployer_fee_scale=1,
        is_delisted=False,
        dex_index=1,
        universe_index=2,
        asset_id=110002,
    )
    book = OrderBook()
    book.apply_hl(
        [[{"px": "99.99", "sz": "10"}], [{"px": "100.01", "sz": "10"}]]
    )
    engine.snapshot = MarketSnapshot(meta, book, mark_px=100.0, oracle_px=100.0)
    account = AccountState(
        position_qty=(cfg.risk.max_position_usd * 1.03) / 100.0,
        equity_actual=1000.0,
        withdrawable=1000.0,
    )

    def order(role: str, is_buy: bool) -> TrackedOrder:
        return TrackedOrder(
            role=role,
            cloid="0x" + ("5" if is_buy else "6") * 32,
            is_buy=is_buy,
            price=99.99 if is_buy else 100.01,
            qty=cfg.market_maker.order_notional_usd / 100.0,
            reduce_only=False,
            tif="Alo",
            created_ts=0.0,
        )

    class Broker:
        def __init__(self):
            self.account = account
            self.orders_by_role = {
                "mm:bid:0": order("mm:bid:0", True),
                "mm:ask:0": order("mm:ask:0", False),
            }
            self.canceled = []

        async def cancel_roles(self, roles):
            self.canceled.extend(roles)
            for role in roles:
                self.orders_by_role.pop(role, None)

    broker = Broker()
    engine.broker = broker  # type: ignore[assignment]
    engine.strategy = SimpleNamespace(halt_reason=lambda _market, _account: None)
    engine.start_equity = 1000.0

    assert asyncio.run(engine._prepare_position_recovery()) is True
    assert set(broker.canceled) == {"mm:bid:0", "mm:ask:0"}
    assert broker.orders_by_role == {}
    assert engine._risk_reason() is None

    # A materially larger breach stays fail-closed and is not auto-repaired.
    account.position_qty = (
        cfg.risk.max_position_usd
        + cfg.market_maker.order_notional_usd * 1.11
    ) / 100.0
    broker.orders_by_role = {"mm:bid:0": order("mm:bid:0", True)}
    broker.canceled.clear()
    assert asyncio.run(engine._prepare_position_recovery()) is False
    assert broker.canceled == []
    assert "超过上限" in engine._risk_reason()


def test_principal_drawdown_guard_is_not_reset_by_a_new_session():
    cfg = load_config(Path(__file__).parents[1] / "config.market_maker.json")
    cfg = replace(
        cfg,
        risk=replace(
            cfg.risk,
            capital_usd=1000.0,
            max_session_drawdown_usd=10.0,
            max_principal_drawdown_usd=20.0,
        ),
    )
    engine = Engine(cfg)
    book = OrderBook()
    book.apply_hl(
        [[{"px": "99.99", "sz": "10"}], [{"px": "100.01", "sz": "10"}]]
    )
    engine.snapshot = SimpleNamespace(
        book=book,
        oracle_px=100.0,
        fair_px=100.0,
    )
    account = AccountState(
        position_qty=0.0,
        equity_actual=980.0,
        withdrawable=900.0,
    )
    engine.broker = SimpleNamespace(account=account, orders_by_role={})
    engine.strategy = SimpleNamespace(halt_reason=lambda _market, _account: None)
    engine.start_equity = 980.0

    assert "本金累计回撤 $20.00" in engine._risk_reason()

    account.equity_actual = 980.01
    engine.start_equity = 980.01
    assert engine._risk_reason() is None


def test_liquidation_distance_guard_halts_before_margin_breach():
    """Margin backstop: a liquidation price inside 15% of mark means the
    leverage/margin state is not what this configuration assumes; halt (and
    flatten) long before the exchange can liquidate."""
    cfg = load_config(Path(__file__).parents[1] / "config.market_maker.json")
    engine = Engine(cfg)
    book = OrderBook()
    book.apply_hl(
        [[{"px": "99.99", "sz": "10"}], [{"px": "100.01", "sz": "10"}]]
    )
    engine.snapshot = SimpleNamespace(
        book=book,
        oracle_px=100.0,
        fair_px=100.0,
    )
    account = AccountState(
        position_qty=0.4,
        equity_actual=cfg.risk.capital_usd,
        withdrawable=cfg.risk.capital_usd,
        liquidation_px=93.0,  # 7% away: inside the 10% floor
    )
    engine.broker = SimpleNamespace(account=account, orders_by_role={})
    engine.strategy = SimpleNamespace(halt_reason=lambda _market, _account: None)
    engine.start_equity = cfg.risk.capital_usd

    assert "强平价距离" in str(engine._risk_reason())

    # A healthy 5X-isolated distance (~18%) passes.
    account.liquidation_px = 82.0
    assert engine._risk_reason() is None

    # Flat book position: guard does not apply.
    account.liquidation_px = 99.0
    account.position_qty = 0.0
    assert engine._risk_reason() is None


def _risk_gate_engine(cfg):
    engine = Engine(cfg)
    book = OrderBook()
    book.apply_hl(
        [[{"px": "99.99", "sz": "10"}], [{"px": "100.01", "sz": "10"}]]
    )
    engine.snapshot = SimpleNamespace(book=book, oracle_px=100.0, fair_px=100.0)
    account = AccountState(
        position_qty=0.0,
        equity_actual=cfg.risk.capital_usd,
        withdrawable=cfg.risk.capital_usd,
    )
    engine.broker = SimpleNamespace(account=account, orders_by_role={})
    engine.strategy = SimpleNamespace(halt_reason=lambda _market, _account: None)
    engine.start_equity = cfg.risk.capital_usd
    return engine, account


def test_wide_book_pauses_instead_of_halting():
    """A wide book is a market state: pause with hysteresis instead of halt.

    The account risk gate must not treat temporary liquidity loss as a
    permanent stop condition."""
    cfg = load_config(Path(__file__).parents[1] / "config.market_maker.json")
    engine, _account = _risk_gate_engine(cfg)

    # Wide book: 99 vs 101 mid 100 -> 200 bps spread, far above the 25 gate.
    engine.snapshot.book.apply_hl(
        [[{"px": "99.0", "sz": "10"}], [{"px": "101.0", "sz": "10"}]]
    )
    assert engine._risk_reason() is None, "wide spread must not halt"
    assert "盘口价差" in str(engine._market_quality_pause_reason())

    # Hysteresis: paused state requires spread <= 80% of the gate to resume.
    engine._market_quality_paused = True
    engine.snapshot.book.apply_hl(
        # ~22 bps spread: below the 25 entry gate but above the 20 resume gate.
        [[{"px": "99.89", "sz": "10"}], [{"px": "100.11", "sz": "10"}]]
    )
    assert engine._market_quality_pause_reason() is not None

    engine.snapshot.book.apply_hl(
        # ~10 bps: comfortably below the resume gate.
        [[{"px": "99.95", "sz": "10"}], [{"px": "100.05", "sz": "10"}]]
    )
    assert engine._market_quality_pause_reason() is None


def test_aged_inventory_triggers_bounded_ioc_reduction(monkeypatch):
    """Stage 3: a position stuck past the rebalance threshold for over five
    minutes gets one bounded IOC clip per minute until back inside. Splitting
    the same ladder budget across more levels must not enlarge the taker clip."""
    cfg = load_config(Path(__file__).parents[1] / "config.market_maker.json")
    cfg = replace(cfg, market_maker=replace(cfg.market_maker, levels=3))
    engine = Engine(cfg)
    book = OrderBook()
    book.apply_hl(
        [[{"px": "99.99", "sz": "10"}], [{"px": "100.01", "sz": "10"}]]
    )
    engine.snapshot = SimpleNamespace(book=book, fair_px=100.0, oracle_px=100.0)
    calls = []

    class Broker:
        account = AccountState(
            position_qty=cfg.risk.max_position_usd * 0.6 / 100.0
        )
        orders_by_role: dict = {}

        async def reduce_position_ioc(self, _book, qty, slippage_bps):
            calls.append((qty, slippage_bps))

    engine.broker = Broker()  # type: ignore[assignment]

    # First sighting arms the timer; nothing fires yet.
    asyncio.run(engine._manage_aged_inventory())
    assert calls == []

    # Aged past the window: one clip fires, sized to half the ladder.
    engine._inventory_breach_since = time.monotonic() - 301.0
    asyncio.run(engine._manage_aged_inventory())
    assert len(calls) == 1
    expected_qty = (
        cfg.market_maker.order_notional_usd / 2.0
    ) / 100.0
    assert abs(calls[0][0] - expected_qty) < 1e-9
    assert calls[0][1] == 15.0

    # Interval throttle: no second clip within a minute.
    asyncio.run(engine._manage_aged_inventory())
    assert len(calls) == 1

    # Back inside the threshold: timer resets.
    engine.broker.account.position_qty = 0.0
    asyncio.run(engine._manage_aged_inventory())
    assert engine._inventory_breach_since is None


def test_crisis_quotes_require_live_oracle_anchor(monkeypatch):
    """The far-touch anchor must be a MOVING oracle: a weekend equity feed
    that keeps pushing the stale Friday close never qualifies."""
    cfg = load_config(Path(__file__).parents[1] / "config.market_maker.json")
    cfg = replace(
        cfg,
        market_maker=replace(
            cfg.market_maker, crisis_quote_notional_usd=400.0
        ),
    )
    engine, _account = _risk_gate_engine(cfg)
    engine.market = SimpleNamespace(ws_ctx_age_sec=5.0)  # type: ignore[assignment]

    # No oracle change observed yet.
    assert engine._crisis_quotes_allowed() is False

    # Live anchor: value changed moments ago.
    engine._track_oracle_liveness()
    assert engine._crisis_quotes_allowed() is True

    # Stale anchor: same value for over ten minutes.
    engine._oracle_changed_at = time.monotonic() - 601.0
    assert engine._crisis_quotes_allowed() is False

    # Stale push: context feed itself is old.
    engine._oracle_changed_at = time.monotonic()
    engine.market.ws_ctx_age_sec = 120.0
    assert engine._crisis_quotes_allowed() is False

    # Disabled by notional.
    engine.market.ws_ctx_age_sec = 5.0
    engine.cfg = replace(
        engine.cfg,
        market_maker=replace(
            engine.cfg.market_maker, crisis_quote_notional_usd=0.0
        ),
    )
    assert engine._crisis_quotes_allowed() is False


def test_oracle_drift_routes_to_pause_channel():
    cfg = load_config(Path(__file__).parents[1] / "config.market_maker.json")
    engine, _account = _risk_gate_engine(cfg)
    # Tight book far away from the oracle: mid 100 vs oracle 98 = ~204 bps,
    # beyond the 150 bps deviation gate.
    engine.snapshot.oracle_px = 98.0
    assert engine._risk_reason() is None
    assert "oracle" in str(engine._market_quality_pause_reason())


def test_rolling_drawdown_line_flags_fast_loss_only():
    """The incident line reacts to loss velocity: the same $12 loss passes
    when spread over time but halts when it happens inside the window."""
    cfg = load_config(Path(__file__).parents[1] / "config.market_maker.json")
    cfg = replace(
        cfg,
        risk=replace(
            cfg.risk,
            max_rolling_drawdown_usd=10.0,
            daily_loss_budget_usd=0.0,  # Isolate the rolling-window incident guard.
            rolling_drawdown_window_min=30.0,
            max_session_drawdown_usd=1000.0,
            max_principal_drawdown_usd=400.0,
            capital_usd=1000.0,
        ),
    )
    engine, account = _risk_gate_engine(cfg)

    assert engine._risk_reason() is None
    # Slow bleed: the old peak has rolled out of the 30-minute window.
    engine._equity_history.clear()
    engine._equity_history.append((time.monotonic() - 10.0, 995.0))
    account.equity_actual = 992.0
    assert engine._risk_reason() is None

    # Fast loss: $12 inside the window trips the incident line.
    engine._equity_history.append((time.monotonic() - 5.0, 1000.0))
    account.equity_actual = 988.0
    assert "快速回撤" in str(engine._risk_reason())


def test_daily_loss_budget_halts_and_survives_restart(tmp_path):
    """Slow wear beyond the Beijing-day budget halts with the budget reason,
    and a restart on the same day must not refill the budget."""
    cfg = load_config(Path(__file__).parents[1] / "config.market_maker.json")
    cfg = replace(
        cfg,
        risk=replace(
            cfg.risk,
            daily_loss_budget_usd=50.0,
            max_rolling_drawdown_usd=0.0,  # Isolate the persistent daily budget.
            max_session_drawdown_usd=1000.0,
            max_principal_drawdown_usd=400.0,
            capital_usd=1000.0,
        ),
    )
    engine, account = _risk_gate_engine(cfg)
    engine.ledger = SimpleNamespace(state_path=tmp_path / "x_state.json")
    engine.start_equity = 1000.0

    account.equity_actual = 960.0  # -$40 of wear today: inside budget
    assert engine._risk_reason() is None
    baseline_file = json.loads(
        (tmp_path / "daily_loss_budget.json").read_text(encoding="utf-8")
    )
    assert baseline_file["day_start_equity"] == 960.0

    # Wait: the baseline was recorded at first check (960).  Losing $50 more
    # from there exhausts the budget.
    account.equity_actual = 909.0
    assert "当日亏损预算已用尽" in str(engine._risk_reason())

    # A fresh engine the same day reads the shared baseline instead of
    # resetting it to current equity.
    engine2, account2 = _risk_gate_engine(cfg)
    engine2.ledger = SimpleNamespace(state_path=tmp_path / "x_state.json")
    engine2.start_equity = 909.0
    account2.equity_actual = 909.0
    assert "当日亏损预算已用尽" in str(engine2._risk_reason())


def test_halt_flatten_retries_with_fresh_book_then_flags_naked_position(
    monkeypatch,
):
    cfg = load_config(Path(__file__).parents[1] / "config.market_maker.json")
    cfg = replace(cfg, risk=replace(cfg.risk, flatten_on_halt=True))
    monkeypatch.setattr("entropy_bot.engine.FLATTEN_RETRY_DELAY_SEC", 0.0)

    def build_engine(flatten_behavior):
        engine = Engine(cfg)
        book = OrderBook()
        book.apply_hl(
            [[{"px": "99.99", "sz": "10"}], [{"px": "100.01", "sz": "10"}]]
        )
        engine.snapshot = SimpleNamespace(book=book)
        flatten_calls = []
        book_refreshes = []

        class Broker:
            orders_by_role: dict = {}

            async def flatten(self, _book, _slippage):
                flatten_calls.append(1)
                return flatten_behavior(len(flatten_calls))

        class Market:
            async def rest_book_once(self):
                book_refreshes.append(1)
                return book

        engine.broker = Broker()  # type: ignore[assignment]
        engine.market = Market()  # type: ignore[assignment]
        summaries = []
        engine._write_summary = (  # type: ignore[method-assign]
            lambda status, reason=None: summaries.append((status, reason))
        )

        async def fake_exit(_context):
            return True

        engine._cancel_and_reconcile_exit = fake_exit  # type: ignore[method-assign]
        return engine, flatten_calls, book_refreshes, summaries

    # All attempts fail: reason must flag the possibly-naked position.
    def always_fail(_n):
        raise RuntimeError("盘口不可用")

    engine, calls, refreshes, summaries = build_engine(always_fail)
    asyncio.run(engine.halt("测试停机"))
    assert len(calls) == 3
    assert len(refreshes) == 2  # attempts 2 and 3 refresh the book first
    assert "需要立即人工处理" in str(summaries[-1][1])

    # Second attempt succeeds: no naked-position warning.
    def fail_once(n):
        if n == 1:
            raise RuntimeError("瞬时网络故障")

    engine, calls, refreshes, summaries = build_engine(fail_once)
    asyncio.run(engine.halt("测试停机"))
    assert len(calls) == 2
    assert len(refreshes) == 1
    assert "人工处理" not in str(summaries[-1][1])


def test_halt_never_retries_an_uncertain_ioc_outcome():
    cfg = load_config(Path(__file__).parents[1] / "config.market_maker.json")
    cfg = replace(cfg, risk=replace(cfg.risk, flatten_on_halt=True))
    engine = Engine(cfg)
    book = OrderBook()
    book.apply_hl(
        [[{"px": "99.99", "sz": "10"}], [{"px": "100.01", "sz": "10"}]]
    )
    engine.snapshot = SimpleNamespace(book=book)
    calls = []

    class Broker:
        orders_by_role: dict = {}
        account = AccountState(position_qty=1.0)

        async def flatten(self, _book, _slippage):
            calls.append(1)
            raise UncertainIocOutcome("已保留 cloid，禁止重发")

    engine.broker = Broker()  # type: ignore[assignment]
    summaries = []
    engine._write_summary = (  # type: ignore[method-assign]
        lambda status, reason=None: summaries.append((status, reason))
    )

    async def fake_exit(_context):
        return True

    engine._cancel_and_reconcile_exit = fake_exit  # type: ignore[method-assign]
    asyncio.run(engine.halt("测试停机"))

    assert len(calls) == 1
    assert "禁止重发" in str(summaries[-1][1])
    assert "核对官方仓位" in str(summaries[-1][1])


def test_halt_surfaces_unclean_order_reconciliation():
    cfg = load_config(Path(__file__).parents[1] / "config.market_maker.json")
    cfg = replace(cfg, risk=replace(cfg.risk, flatten_on_halt=False))

    for exit_result in (False, RuntimeError("official order view unavailable")):
        engine = Engine(cfg)
        engine.broker = SimpleNamespace(orders_by_role={"mm:bid:0": object()})  # type: ignore[assignment]
        summaries = []
        engine._write_summary = (  # type: ignore[method-assign]
            lambda status, reason=None: summaries.append((status, reason))
        )

        async def fake_exit(_context, result=exit_result):
            if isinstance(result, Exception):
                raise result
            return result

        engine._cancel_and_reconcile_exit = fake_exit  # type: ignore[method-assign]
        asyncio.run(engine.halt("测试停机"))

        assert summaries[-1][0] == "halted"
        assert "订单终态未确认干净" in str(summaries[-1][1])
        assert "核对官方挂单" in str(summaries[-1][1])


def test_ordinary_exit_promotes_unclean_cancel_or_exception_to_halt():
    cfg = load_config(Path(__file__).parents[1] / "config.market_maker.json")

    for exit_result in (False, RuntimeError("official order view unavailable")):
        engine = Engine(cfg)
        engine.status = "running"
        engine.broker = SimpleNamespace(orders_by_role={"mm:bid:0": object()})  # type: ignore[assignment]

        async def fake_exit(_context, result=exit_result):
            if isinstance(result, Exception):
                raise result
            return result

        engine._cancel_and_reconcile_exit = fake_exit  # type: ignore[method-assign]
        asyncio.run(engine._finalize_exit_order_state())

        assert engine.status == "halted"
        assert "未确认" in str(engine.halt_reason_text)
        assert "核对官方挂单" in str(engine.halt_reason_text)


def test_ordinary_exit_remains_clean_when_order_reconciliation_succeeds():
    cfg = load_config(Path(__file__).parents[1] / "config.market_maker.json")
    engine = Engine(cfg)
    engine.status = "running"
    engine.broker = SimpleNamespace(orders_by_role={})  # type: ignore[assignment]

    async def fake_exit(_context):
        return True

    engine._cancel_and_reconcile_exit = fake_exit  # type: ignore[method-assign]
    asyncio.run(engine._finalize_exit_order_state())

    assert engine.status == "running"
    assert engine.halt_reason_text is None


def test_halt_force_syncs_and_warns_when_flatten_fill_detail_is_delayed():
    cfg = load_config(Path(__file__).parents[1] / "config.market_maker.json")
    cfg = replace(cfg, risk=replace(cfg.risk, flatten_on_halt=True))
    meta = MarketMeta(
        coin="io:SNDK",
        dex="io",
        symbol="SNDK",
        sz_decimals=4,
        max_leverage=10,
        only_isolated=True,
        growth_mode=True,
        deployer_fee_scale=1,
        is_delisted=False,
        dex_index=1,
        universe_index=2,
        asset_id=110002,
    )
    fees = FeeModel(
        user_maker_rate=0.00015,
        user_taker_rate=0.00045,
        active_hl_referral_discount=0.0,
        deployer_fee_scale=1.0,
        deployer_share=0.5,
        growth_mode=True,
        gross_maker_rate=0.00003,
        gross_taker_rate=0.00009,
        entropy_self_rebate_pct=160.0,
        entropy_referred_benefit_pct=0.0,
    )
    broker = LiveBroker(
        cfg,
        meta,
        fees,
        object(),  # type: ignore[arg-type]
        "not-used-in-this-test",
        "0x" + "1" * 40,
    )
    broker.account.position_qty = 1.0
    events = []

    async def fake_flatten(_book, _slippage):
        events.append("flatten")
        broker.account.position_qty = 0.0

    async def fake_sync(_book, *, force=False):
        events.append(("sync", force))

    broker.flatten = fake_flatten  # type: ignore[method-assign]
    broker.sync = fake_sync  # type: ignore[method-assign]

    engine = Engine(cfg)
    book = OrderBook()
    book.apply_hl(
        [[{"px": "99.99", "sz": "10"}], [{"px": "100.01", "sz": "10"}]]
    )
    engine.snapshot = SimpleNamespace(book=book)
    engine.broker = broker
    engine.ledger = SimpleNamespace()
    engine.strategy = SimpleNamespace()
    engine._record_fills = lambda: events.append("record") or False  # type: ignore[method-assign]
    engine._record_due_markouts = lambda: None  # type: ignore[method-assign]
    summaries = []
    engine._write_summary = (  # type: ignore[method-assign]
        lambda status, reason=None: summaries.append((status, reason))
    )

    async def fake_exit(_context):
        return True

    engine._cancel_and_reconcile_exit = fake_exit  # type: ignore[method-assign]
    asyncio.run(engine.halt("测试停机"))

    assert events == ["flatten", ("sync", True), "record"]
    assert "成交明细" in str(summaries[-1][1])
    assert "下次启动前" in str(summaries[-1][1])


def test_transient_stale_book_cancels_quotes_and_waits_for_reconnect(monkeypatch):
    cfg = load_config(Path(__file__).parents[1] / "config.market_maker.json")
    engine = Engine(cfg)

    class Book:
        fresh = False

        def is_fresh(self, _max_age):
            return self.fresh

    book = Book()
    engine.snapshot = SimpleNamespace(book=book)

    class Broker:
        def __init__(self):
            self.orders_by_role = {"mm:bid:0": object(), "mm:ask:0": object()}
            self.canceled = []

        async def cancel_roles(self, roles):
            self.canceled.extend(roles)
            for role in roles:
                self.orders_by_role.pop(role, None)

    broker = Broker()
    engine.broker = broker  # type: ignore[assignment]
    now = [100.0]
    monkeypatch.setattr("entropy_bot.engine.time.monotonic", lambda: now[0])

    assert asyncio.run(engine._book_recovery_pending()) is True
    assert set(broker.canceled) == {"mm:bid:0", "mm:ask:0"}
    # Orders confirmed away: a stale feed is now a wait state, not a halt
    # Confirmed cancellations make prolonged feed recovery safe to wait for.
    now[0] += 16.0
    assert asyncio.run(engine._book_recovery_pending()) is True
    assert engine.status == "paused"
    # Orders still resting past the grace window: fail closed as before.
    broker.orders_by_role["mm:bid:0"] = object()
    assert asyncio.run(engine._book_recovery_pending()) is False

    book.fresh = True
    assert asyncio.run(engine._book_recovery_pending()) is False
    assert engine._book_stale_since is None


def test_connectivity_error_classifier_keeps_programming_errors_hard():
    assert Engine._is_transient_connectivity_error(
        aiohttp.ClientConnectionError("network down")
    )
    assert Engine._is_transient_connectivity_error(asyncio.TimeoutError())
    assert not Engine._is_transient_connectivity_error(ValueError("bad state"))


def test_transient_connectivity_pause_reconciles_before_resuming(monkeypatch):
    cfg = load_config(Path(__file__).parents[1] / "config.market_maker.json")
    engine = Engine(cfg)
    summaries: list[tuple[str, str | None]] = []
    events: list[str] = []

    class Book:
        def is_fresh(self, _max_age):
            return True

    class Market:
        def __init__(self):
            self.book = Book()

        async def refresh_context(self):
            events.append("context")

    class Broker:
        def __init__(self):
            self.orders_by_role = {}
            self.official_foreign_orders = 0

        async def sync(self, _book, *, force=False):
            assert force is True
            events.append("sync")

    engine.market = Market()  # type: ignore[assignment]
    engine.snapshot = SimpleNamespace(book=engine.market.book)  # type: ignore[assignment]
    engine.broker = Broker()  # type: ignore[assignment]
    attempts = iter([False, True])

    async def fake_cancel(*_args, **_kwargs):
        events.append("cancel")
        return next(attempts)

    async def fake_reconcile(*, force=False):
        assert force is True
        events.append("reconcile")

    async def no_sleep(_seconds):
        return None

    engine._cancel_and_reconcile_exit = fake_cancel  # type: ignore[method-assign]
    engine._reconcile = fake_reconcile  # type: ignore[method-assign]
    engine._effective_trading_schedule_state = (  # type: ignore[method-assign]
        lambda: TradingScheduleState(
            False, None, datetime(2026, 8, 29, 15, 0, tzinfo=timezone.utc)
        )
    )
    engine._risk_reason = lambda **_kwargs: None  # type: ignore[method-assign]
    engine._record_fills = lambda: False  # type: ignore[method-assign]
    engine._record_due_markouts = lambda: None  # type: ignore[method-assign]
    engine._apply_dynamic_sizing = lambda: False  # type: ignore[method-assign]
    engine._write_summary = (  # type: ignore[method-assign]
        lambda status, reason=None: summaries.append((status, reason))
    )
    monkeypatch.setattr("entropy_bot.engine.asyncio.sleep", no_sleep)

    recovered = asyncio.run(
        engine._recover_transient_connectivity(
            aiohttp.ClientConnectionError("network down")
        )
    )

    assert recovered is True
    assert engine.status == "running"
    assert events == ["cancel", "cancel", "context", "sync", "reconcile"]
    assert summaries[-1] == ("running", None)


def test_book_staleness_never_halts_risk_gate():
    """A stale feed is a wait state owned by _book_recovery_pending.

    The risk gate must pass with an unready book so a temporary outage
    does not escalate to a permanent halt."""
    cfg = load_config(Path(__file__).parents[1] / "config.market_maker.json")
    engine = Engine(cfg)
    book = OrderBook()
    book.apply_hl(
        [[{"px": "99.99", "sz": "10"}], [{"px": "100.01", "sz": "10"}]]
    )
    book.ready = False
    engine.snapshot = SimpleNamespace(
        book=book,
        oracle_px=100.0,
        fair_px=100.0,
    )
    engine.broker = SimpleNamespace(
        account=AccountState(
            equity_actual=cfg.risk.capital_usd,
            withdrawable=cfg.risk.capital_usd,
        ),
        orders_by_role={},
    )
    engine.strategy = SimpleNamespace(halt_reason=lambda _market, _account: None)
    engine.start_equity = cfg.risk.capital_usd

    assert engine._risk_reason() is None
    assert engine._risk_reason(ignore_book_staleness=True) is None


def test_markout_uses_first_websocket_mid_at_or_after_target():
    cfg = load_config(Path(__file__).parents[1] / "config.market_maker.json")
    engine = Engine(cfg)
    captured = []
    engine.ledger = SimpleNamespace(
        record_markout=lambda fill, horizon, mid, observed_ms=None: captured.append(
            (fill.fill_id, horizon, mid, observed_ms)
        )
    )
    engine.snapshot = SimpleNamespace()
    fill = Fill(
        fill_id="fill-target-mid",
        order_role="mm:bid:0",
        cloid="0x" + "9" * 32,
        oid=9,
        is_buy=True,
        price=100.0,
        qty=0.1,
        fee_usd=0.0,
        liquidity="maker",
        time_ms=1_000,
    )
    engine._pending_markouts[fill.fill_id] = {"fill": fill, "remaining": {1}}
    engine._mid_history.extend([(1_999, 99.0), (2_005, 101.0), (2_010, 102.0)])

    engine._record_due_markouts()

    assert captured == [("fill-target-mid", 1, 101.0, 2_005)]
    assert engine._pending_markouts == {}
