from dataclasses import replace
from pathlib import Path

import pytest

from entropy_bot.config import load_config
from entropy_bot.fees import build_fee_model
from entropy_bot.models import AccountState, Fill, MarketMeta, MarketSnapshot, OrderBook
from entropy_bot.strategies import GridStrategy, MarketMakerStrategy


def setup(tmp_path, strategy="market_maker"):
    source = Path(__file__).parents[1] / f"config.{strategy}.json"
    cfg = load_config(source)
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
    market = MarketSnapshot(meta, book, mark_px=100, oracle_px=100)
    fees = build_fee_model(meta, None, self_rebate_pct=160, referred_benefit_pct=0)
    return cfg, market, fees


def test_market_maker_quotes_are_post_only_and_capped(tmp_path):
    cfg, market, fees = setup(tmp_path)
    s = MarketMakerStrategy(cfg, fees)
    intents = s.desired(market, AccountState(equity_actual=1000))
    assert intents
    assert all(x.price < market.book.best_ask for x in intents if x.is_buy)
    assert all(x.price > market.book.best_bid for x in intents if not x.is_buy)
    assert sum(x.notional for x in intents) <= cfg.risk.max_open_order_usd + 1e-6


def test_inventory_skew_moves_quotes_down_when_long(tmp_path):
    cfg, market, fees = setup(tmp_path)
    s = MarketMakerStrategy(cfg, fees)
    flat = {x.role: x.price for x in s.desired(market, AccountState())}
    long = {
        x.role: x.price
        for x in s.desired(market, AccountState(position_qty=1.5))
    }
    common = set(flat) & set(long)
    assert common
    assert all(long[k] <= flat[k] for k in common)


def test_market_maker_keeps_both_sides_when_oracle_lags_book(tmp_path):
    cfg, market, fees = setup(tmp_path)
    market.book.apply_hl(
        [[{"px": "101.00", "sz": "10"}], [{"px": "101.02", "sz": "10"}]]
    )
    market.mark_px = market.oracle_px = 100.0
    intents = MarketMakerStrategy(cfg, fees).desired(market, AccountState())
    assert any(x.is_buy for x in intents)
    assert any(not x.is_buy for x in intents)
    assert all(x.price < market.book.best_ask for x in intents if x.is_buy)
    assert all(x.price > market.book.best_bid for x in intents if not x.is_buy)


def test_market_maker_pegs_inventory_reducing_side_to_bbo(tmp_path):
    cfg, market, fees = setup(tmp_path)
    market.book.apply_hl(
        [[{"px": "101.00", "sz": "10"}], [{"px": "101.02", "sz": "10"}]]
    )
    market.mark_px = market.oracle_px = 100.0
    strategy = MarketMakerStrategy(cfg, fees)

    qty_at_sixty_pct = cfg.risk.max_position_usd * 0.60 / market.fair_px
    short = strategy.desired(
        market, AccountState(position_qty=-qty_at_sixty_pct)
    )
    long = strategy.desired(
        market, AccountState(position_qty=qty_at_sixty_pct)
    )

    assert next(item for item in short if item.is_buy).price == 101.00
    assert next(item for item in long if not item.is_buy).price == 101.02
    assert next(item for item in short if item.is_buy).reduce_only is True
    assert next(item for item in long if not item.is_buy).reduce_only is True


def test_market_maker_adaptive_spread_waits_for_sample_gate_and_persists(tmp_path):
    cfg, market, fees = setup(tmp_path)
    cfg = replace(
        cfg,
        market_maker=replace(
            cfg.market_maker,
            adaptive_spread_enabled=True,
            adaptive_spread_min_samples=20,
            adaptive_spread_window_samples=40,
            adaptive_spread_adjust_every_samples=20,
            adaptive_spread_wear_trigger_bps=1.0,
            adaptive_spread_step_bps=0.5,
            adaptive_spread_max_extra_bps=2.0,
        ),
    )
    strategy = MarketMakerStrategy(cfg, fees)
    adverse = {
        "horizon_sec": 30,
        "notional_usd": 100.0,
        "markout_pnl_usd": -0.02,
    }

    for _ in range(19):
        assert strategy.on_markout(adverse) is False
    assert strategy.adaptive_extra_bps == 0.0
    assert strategy.on_markout(adverse) is True
    assert strategy.adaptive_extra_bps == 0.5

    saved = strategy.state_dict()
    restored = MarketMakerStrategy(cfg, fees)
    restored.load_state(saved)
    assert restored.adaptive_extra_bps == 0.5
    assert restored.state_dict()["adaptive_markouts"] == saved["adaptive_markouts"]


def test_market_maker_adaptive_spread_keeps_low_wear_quote_unchanged(tmp_path):
    cfg, _market, fees = setup(tmp_path)
    cfg = replace(
        cfg,
        market_maker=replace(
            cfg.market_maker,
            adaptive_spread_enabled=True,
            adaptive_spread_min_samples=20,
            adaptive_spread_window_samples=40,
            adaptive_spread_adjust_every_samples=20,
            adaptive_spread_wear_trigger_bps=1.0,
            adaptive_spread_step_bps=0.5,
            adaptive_spread_max_extra_bps=2.0,
        ),
    )
    strategy = MarketMakerStrategy(cfg, fees)
    low_wear = {
        "horizon_sec": 30,
        "notional_usd": 100.0,
        "markout_pnl_usd": 0.0,
    }

    for _ in range(20):
        assert strategy.on_markout(low_wear) is False

    saved = strategy.state_dict()
    assert strategy.adaptive_extra_bps == 0.0
    assert saved["adaptive_samples_since_evaluation"] == 0
    assert len(saved["adaptive_markouts"]) == 20


def test_entry_side_stops_quoting_at_heavy_inventory(tmp_path):
    """At the rebalance target the entry side is gone completely."""
    cfg, market, fees = setup(tmp_path)
    cfg = replace(
        cfg,
        # The 40%-to-50% entry room must accommodate a real minimum-size order.
        risk=replace(cfg.risk, max_position_usd=1000.0, max_open_order_usd=4000.0, min_order_notional_usd=10.0),
        market_maker=replace(cfg.market_maker, order_notional_usd=100.0, levels=1, rebalance_at_position_ratio=0.5),
    )
    strategy = MarketMakerStrategy(cfg, fees)
    heavy_long = cfg.risk.max_position_usd * 0.90 / market.fair_px

    intents = strategy.desired(market, AccountState(position_qty=heavy_long))

    assert intents, "reducing side must still quote"
    assert all(not item.is_buy for item in intents)
    assert all(item.reduce_only for item in intents)

    intents_short = strategy.desired(
        market, AccountState(position_qty=-heavy_long)
    )
    assert intents_short and all(item.is_buy for item in intents_short)

    # Below the target both sides still quote, but all entry fills together
    # cannot push inventory beyond that target.
    moderate = cfg.risk.max_position_usd * 0.40 / market.fair_px
    both = strategy.desired(market, AccountState(position_qty=moderate))
    assert any(item.is_buy for item in both)
    assert any(not item.is_buy for item in both)
    entry_room = (
        cfg.risk.max_position_usd * cfg.market_maker.rebalance_at_position_ratio
        - moderate * market.fair_px
    )
    assert sum(item.notional for item in both if item.is_buy) <= entry_room + 1.0


def test_volatility_guard_quotes_reduce_only_side_until_range_halves(tmp_path):
    """A wide 60 s realized range flips quoting to exposure-reducing only
    (nothing at all when flat) and releases with hysteresis."""
    import time as _time

    cfg, market, fees = setup(tmp_path)
    cfg = replace(
        cfg,
        market_maker=replace(cfg.market_maker, volatility_guard_range_bps=20.0),
    )
    strategy = MarketMakerStrategy(cfg, fees)
    mid = market.book.mid

    # Seed a 60s window whose range is ~50 bps (> 20 bps threshold).
    now = _time.monotonic()
    strategy._mid_history.append((now - 10.0, mid * 0.995))

    flat = strategy.desired(market, AccountState())
    assert strategy._volatility_guard_active is True
    assert flat == []

    long_qty = cfg.risk.max_position_usd * 0.4 / market.fair_px
    reducing = strategy.desired(market, AccountState(position_qty=long_qty))
    assert reducing and all(not item.is_buy for item in reducing)

    # Range collapses: only fresh, tight mids remain -> guard releases.
    strategy._mid_history.clear()
    strategy._mid_history.append((_time.monotonic(), mid))
    recovered = strategy.desired(market, AccountState())
    assert strategy._volatility_guard_active is False
    assert any(item.is_buy for item in recovered)
    assert any(not item.is_buy for item in recovered)


def _mm_fill(role: str, is_buy: bool) -> Fill:
    return Fill(
        fill_id="cooldown-test",
        order_role=role,
        cloid="0x" + "1" * 32,
        oid=7,
        is_buy=is_buy,
        price=100.0,
        qty=1.0,
        fee_usd=0.0,
        liquidity="maker",
        time_ms=1,
    )


def test_same_side_fill_cooldown_blocks_entry_side_only(tmp_path):
    """After a fill the exposure-increasing side must pause briefly (same-side
    sweeps carry ~5x the adverse selection), while the reducing side keeps
    quoting so inventory management is never delayed."""
    import time as _time

    cfg, market, fees = setup(tmp_path)
    strategy = MarketMakerStrategy(cfg, fees)
    assert cfg.market_maker.same_side_fill_cooldown_sec == 10.0

    strategy.on_fill(_mm_fill("mm:bid:0", is_buy=True))

    # Flat book: the freshly-filled buy side pauses, sells keep quoting.
    intents = strategy.desired(market, AccountState())
    assert all(not item.is_buy for item in intents)
    assert intents

    # Short position: a buy REDUCES exposure, so the cooldown must not apply.
    short_qty = -cfg.risk.max_position_usd * 0.3 / market.fair_px
    reducing = strategy.desired(market, AccountState(position_qty=short_qty))
    assert any(item.is_buy for item in reducing)

    # Cooldown expiry restores two-sided quoting.
    strategy._last_fill_at[True] = _time.monotonic() - 11.0
    recovered = strategy.desired(market, AccountState())
    assert any(item.is_buy for item in recovered)
    assert any(not item.is_buy for item in recovered)


def test_two_level_ladder_spaces_quotes_and_consolidates_exit(tmp_path):
    """Extra levels split one side budget; rebalance leaves only a full exit."""
    cfg, market, fees = setup(tmp_path)
    cfg = replace(
        cfg,
        risk=replace(
            cfg.risk,
            capital_usd=4000.0,
            max_position_usd=2000.0,
            max_open_order_usd=2000.0,
        ),
        market_maker=replace(
            cfg.market_maker,
            levels=2,
            order_notional_usd=200.0,
            half_spread_bps=2.0,
            level_spacing_bps=4.0,
        ),
    )
    strategy = MarketMakerStrategy(cfg, fees)

    flat = strategy.desired(market, AccountState(equity_actual=4000.0))
    bids = sorted((x for x in flat if x.is_buy), key=lambda x: -x.price)
    asks = sorted((x for x in flat if not x.is_buy), key=lambda x: x.price)
    assert len(bids) == 2 and len(asks) == 2
    assert {x.role for x in bids} == {"mm:bid:0", "mm:bid:1"}
    # Level 1 sits ~spacing further out than level 0 on each side.
    assert bids[1].price < bids[0].price
    assert asks[1].price > asks[0].price
    assert not any(x.reduce_only for x in flat)
    assert sum(x.notional for x in bids) == pytest.approx(200.0, abs=1.0)
    assert sum(x.notional for x in asks) == pytest.approx(200.0, abs=1.0)
    assert bids[0].notional > bids[1].notional

    # Heavy long (60% of cap): only one full reduce-only exit remains.
    heavy = cfg.risk.max_position_usd * 0.60 / market.fair_px
    rebal = strategy.desired(
        market, AccountState(position_qty=heavy, equity_actual=4000.0)
    )
    exits = [x for x in rebal if not x.is_buy]
    assert len(exits) == 1
    assert exits[0].reduce_only and exits[0].price == market.book.best_ask
    assert exits[0].notional == pytest.approx(heavy * exits[0].price, abs=1.0)
    assert len([x for x in rebal if x.is_buy]) == 0


def test_microprice_leans_toward_the_heavy_queue():
    from entropy_bot.models import OrderBook

    book = OrderBook()
    # Heavy bid queue (buy pressure): fair value sits above the plain mid.
    book.apply_hl(
        [[{"px": "99.0", "sz": "30"}], [{"px": "101.0", "sz": "10"}]]
    )
    assert book.mid == 100.0
    assert book.microprice > 100.0
    # Symmetric queues degrade to the mid.
    book.apply_hl(
        [[{"px": "99.0", "sz": "10"}], [{"px": "101.0", "sz": "10"}]]
    )
    assert book.microprice == 100.0


def test_sigma_scaling_widens_spread_skew_and_spacing(tmp_path):
    """A-S structure: realized volatility widens the half-spread above its
    floor, amplifies the inventory skew unit and stretches ladder spacing;
    with zero sigma everything falls back to the configured statics."""
    cfg, market, fees = setup(tmp_path)
    cfg = replace(
        cfg,
        risk=replace(
            cfg.risk,
            capital_usd=4000.0,
            max_position_usd=2000.0,
            max_open_order_usd=2000.0,
        ),
        market_maker=replace(
            cfg.market_maker,
            levels=2,
            order_notional_usd=200.0,
            half_spread_bps=1.0,
            level_spacing_bps=3.0,
            vol_spread_mult=1.0,
            vol_skew_mult=1.0,
            inventory_skew_bps_at_limit=3.0,
        ),
    )
    market.book.apply_hl(
        [[{"px": "99.99", "sz": "10"}], [{"px": "100.01", "sz": "10"}]]
    )
    market.mark_px = market.oracle_px = 100.0

    calm = MarketMakerStrategy(cfg, fees)
    calm_quotes = calm.desired(market, AccountState(equity_actual=4000.0))
    calm_bid = max(x.price for x in calm_quotes if x.is_buy)

    stormy = MarketMakerStrategy(cfg, fees)
    # sigma_h = sqrt(1.0 * 60) ~ 7.7 bps -> vol term dominates the 1 bps floor.
    stormy._sigma_var_bps2 = 1.0
    assert stormy.sigma_horizon_bps > 7.0
    storm_quotes = stormy.desired(market, AccountState(equity_actual=4000.0))
    storm_bid = max(x.price for x in storm_quotes if x.is_buy)
    assert storm_bid < calm_bid  # wider spread in volatility

    # Ladder spacing stretches with sigma too.
    calm_bids = sorted((x.price for x in calm_quotes if x.is_buy), reverse=True)
    storm_bids = sorted((x.price for x in storm_quotes if x.is_buy), reverse=True)
    assert (storm_bids[0] - storm_bids[1]) > (calm_bids[0] - calm_bids[1])

    # Volatility-scaled inventory skew pushes the center down harder when
    # long in a storm than in calm.
    long_qty = 800.0 / 100.0
    calm_long_bid = max(
        x.price
        for x in calm.desired(
            market, AccountState(position_qty=long_qty, equity_actual=4000.0)
        )
        if x.is_buy
    )
    storm_long_bid = max(
        x.price
        for x in stormy.desired(
            market, AccountState(position_qty=long_qty, equity_actual=4000.0)
        )
        if x.is_buy
    )
    assert storm_long_bid < calm_long_bid


def test_aged_inventory_exit_jumps_inside_the_spread(tmp_path):
    """A trapped position must not queue at its own side forever.

    After the aging window, the reduce-only exit moves to the front of
    the spread so the next counter-trade can fill it first."""
    import time as _time

    cfg, market, fees = setup(tmp_path)
    strategy = MarketMakerStrategy(cfg, fees)
    market.book.apply_hl(
        [[{"px": "99.90", "sz": "10"}], [{"px": "100.10", "sz": "10"}]]
    )
    market.mark_px = market.oracle_px = 100.0
    heavy_short = -cfg.risk.max_position_usd * 0.60 / market.fair_px

    fresh = strategy.desired(market, AccountState(position_qty=heavy_short))
    fresh_exit = next(x for x in fresh if x.is_buy)
    assert fresh_exit.reduce_only and fresh_exit.price == 99.90  # own side

    # Age the breach past the improvement window: exit jumps inside spread.
    strategy._rebalance_breach_since = _time.monotonic() - 121.0
    aged = strategy.desired(market, AccountState(position_qty=heavy_short))
    aged_exit = next(x for x in aged if x.is_buy)
    assert aged_exit.reduce_only
    assert 99.90 < aged_exit.price < 100.10  # front of the spread, still ALO

    # Position back inside the threshold resets the aging clock.
    strategy.desired(market, AccountState(position_qty=0.0))
    assert strategy._rebalance_breach_since is None


def test_crisis_quotes_park_far_touch_around_live_oracle(tmp_path):
    """Dislocated book: quotes anchor to the oracle at the configured
    distance, ignore the volatility guard (crisis IS the volatile state),
    and switch to a reduce-only reversion exit once inventory is on."""
    cfg, market, fees = setup(tmp_path)
    cfg = replace(
        cfg,
        risk=replace(
            cfg.risk,
            capital_usd=4000.0,
            max_position_usd=2000.0,
            max_open_order_usd=2000.0,
        ),
        market_maker=replace(
            cfg.market_maker,
            crisis_quote_notional_usd=400.0,
            crisis_quote_distance_bps=80.0,
        ),
    )
    strategy = MarketMakerStrategy(cfg, fees)
    strategy.crisis_mode = True
    strategy._volatility_guard_active = True  # must not block crisis quotes

    # Wide book around oracle 100: bid 99 / ask 101.
    market.book.apply_hl(
        [[{"px": "99.0", "sz": "5"}], [{"px": "101.0", "sz": "5"}]]
    )
    market.mark_px = market.oracle_px = 100.0

    flat = strategy.desired(market, AccountState(equity_actual=1000.0))
    assert {x.is_buy for x in flat} == {True, False}
    bid = next(x for x in flat if x.is_buy)
    ask = next(x for x in flat if not x.is_buy)
    assert bid.price <= 99.21 and bid.price >= 99.1  # oracle -80bps, rounded
    assert ask.price >= 100.79 and ask.price <= 100.9
    assert not bid.reduce_only and not ask.reduce_only
    assert bid.notional <= 400.0 + 1e-6

    # Holding 2x notional long: only the reversion exit remains.
    long_qty = 800.0 / 100.0
    exits = strategy.desired(
        market, AccountState(position_qty=long_qty, equity_actual=1000.0)
    )
    assert exits and all(not x.is_buy for x in exits)
    assert all(x.reduce_only for x in exits)
    assert all(x.price > 100.0 for x in exits)  # above oracle: capture edge


def test_crisis_quotes_disabled_at_zero_notional(tmp_path):
    cfg, market, fees = setup(tmp_path)
    strategy = MarketMakerStrategy(cfg, fees)
    strategy.crisis_mode = True
    market.book.apply_hl(
        [[{"px": "99.0", "sz": "5"}], [{"px": "101.0", "sz": "5"}]]
    )
    market.mark_px = market.oracle_px = 100.0
    assert cfg.market_maker.crisis_quote_notional_usd == 0.0
    assert strategy.desired(market, AccountState(equity_actual=1000.0)) == []


def test_market_maker_shock_brake_jumps_to_max_on_toxic_burst(tmp_path):
    """A short burst of adverse fills must trigger the shock brake promptly.

    When short-window wear greatly exceeds the trigger, the extra spread
    must jump to its maximum without waiting for the normal sample count."""
    cfg, _market, fees = setup(tmp_path)
    cfg = replace(
        cfg,
        market_maker=replace(
            cfg.market_maker,
            adaptive_spread_enabled=True,
            adaptive_spread_min_samples=20,
            adaptive_spread_window_samples=40,
            adaptive_spread_adjust_every_samples=20,
            adaptive_spread_wear_trigger_bps=1.0,
            adaptive_spread_step_bps=0.5,
            adaptive_spread_max_extra_bps=2.0,
        ),
    )
    strategy = MarketMakerStrategy(cfg, fees)
    # -5 bps per fill: wear far beyond trigger*2.5.
    toxic = {
        "horizon_sec": 30,
        "notional_usd": 100.0,
        "markout_pnl_usd": -0.05,
        "order_role": "mm:bid:0",
        "liquidity": "maker",
    }

    # Emergency/risk IOC wear is reported, but its taker slippage must not
    # widen passive maker quotes or trip the shock brake.
    risk_exit = {
        **toxic,
        "order_role": "risk:inventory-ioc",
        "liquidity": "taker",
    }
    for _ in range(10):
        assert strategy.on_markout(risk_exit) is False
    assert strategy.adaptive_metrics["window_samples"] == 0
    assert strategy.adaptive_extra_bps == 0.0

    for _ in range(9):
        assert strategy.on_markout(toxic) is False
    assert strategy.adaptive_extra_bps == 0.0
    # Shock window (max(min_samples//5, 10) = 10) full: brake fires at once.
    assert strategy.on_markout(toxic) is True
    assert strategy.adaptive_extra_bps == 2.0


def test_market_maker_adaptive_spread_decays_after_wear_recovers(tmp_path):
    """The extra spread must step back down once wear is comfortably under
    the trigger, so volume recovers instead of quoting storm-width forever."""
    cfg, _market, fees = setup(tmp_path)
    cfg = replace(
        cfg,
        market_maker=replace(
            cfg.market_maker,
            adaptive_spread_enabled=True,
            adaptive_spread_min_samples=20,
            adaptive_spread_window_samples=40,
            adaptive_spread_adjust_every_samples=20,
            adaptive_spread_wear_trigger_bps=1.0,
            adaptive_spread_step_bps=0.5,
            adaptive_spread_max_extra_bps=2.0,
        ),
    )
    strategy = MarketMakerStrategy(cfg, fees)
    state = strategy.state_dict()
    state["adaptive_extra_bps"] = 2.0
    strategy.load_state(state)
    healthy = {"horizon_sec": 30, "notional_usd": 100.0, "markout_pnl_usd": 0.001}

    for _ in range(19):
        assert strategy.on_markout(healthy) is False
    assert strategy.adaptive_extra_bps == 2.0
    assert strategy.on_markout(healthy) is True
    assert strategy.adaptive_extra_bps == 1.75

    for _ in range(19):
        assert strategy.on_markout(healthy) is False
    assert strategy.on_markout(healthy) is True
    assert strategy.adaptive_extra_bps == 1.5


def test_adaptive_state_is_regime_scoped_and_idle_decay_recovers_volume(
    tmp_path, monkeypatch
):
    cfg, market, fees = setup(tmp_path)
    cfg = replace(
        cfg,
        market_maker=replace(
            cfg.market_maker,
            adaptive_spread_enabled=True,
            adaptive_spread_step_bps=0.5,
            adaptive_spread_idle_decay_sec=1800.0,
            adaptive_spread_sample_max_age_sec=21600.0,
        ),
    )
    strategy = MarketMakerStrategy(cfg, fees)

    # A legacy/different quote regime must not carry its toxic window into the
    # new single-layer baseline.
    strategy.load_state(
        {
            "adaptive_extra_bps": 3.0,
            "adaptive_markouts": [[100.0, -1.0]],
        }
    )
    assert strategy.adaptive_extra_bps == 0.0
    assert strategy.adaptive_metrics["window_samples"] == 0

    # A future one-knob operator experiment must also get a clean adaptive
    # window; cooldown/refresh/skew changes alter which flow reaches the book.
    contaminated = strategy.state_dict()
    contaminated.update(
        {
            "adaptive_extra_bps": 1.0,
            "adaptive_markouts": [[100.0, -0.01, 1000.0]],
        }
    )
    changed_cfg = replace(
        cfg,
        market_maker=replace(
            cfg.market_maker,
            same_side_fill_cooldown_sec=cfg.market_maker.same_side_fill_cooldown_sec
            + 1.0,
        ),
    )
    changed = MarketMakerStrategy(changed_cfg, fees)
    changed.load_state(contaminated)
    assert changed.adaptive_extra_bps == 0.0
    assert changed.adaptive_metrics["window_samples"] == 0

    # Sample lifetime and capital/selector limits also change which markouts
    # are comparable, so each single-knob change must reset the regime.
    additional_changes = [
        replace(
            cfg,
            market_maker=replace(
                cfg.market_maker,
                adaptive_spread_sample_max_age_sec=(
                    cfg.market_maker.adaptive_spread_sample_max_age_sec + 60.0
                ),
            ),
        ),
        replace(
            cfg,
            risk=replace(
                cfg.risk, max_position_usd=cfg.risk.max_position_usd + 100.0
            ),
        ),
        replace(
            cfg,
            runtime=replace(
                cfg.runtime,
                pause_during_us_regular_session=(
                    not cfg.runtime.pause_during_us_regular_session
                ),
            ),
        ),
        replace(
            cfg,
            risk=replace(
                cfg.risk, max_open_order_usd=cfg.risk.max_open_order_usd + 100.0
            ),
        ),
        replace(
            cfg,
            risk=replace(
                cfg.risk,
                min_order_notional_usd=cfg.risk.min_order_notional_usd + 1.0,
            ),
        ),
    ]
    for changed_cfg in additional_changes:
        reset = MarketMakerStrategy(changed_cfg, fees)
        reset.load_state(contaminated)
        assert reset.adaptive_extra_bps == 0.0
        assert reset.adaptive_metrics["window_samples"] == 0

    disabled_cfg = replace(
        cfg,
        market_maker=replace(cfg.market_maker, adaptive_spread_enabled=False),
    )
    disabled = MarketMakerStrategy(disabled_cfg, fees)
    disabled_state = disabled.state_dict()
    disabled_state.update(
        {
            "adaptive_extra_bps": 1.0,
            "adaptive_markouts": [[100.0, -0.01, 1000.0]],
        }
    )
    disabled.load_state(disabled_state)
    assert disabled.adaptive_extra_bps == 0.0
    assert disabled.adaptive_metrics["window_samples"] == 0

    # Same-regime state persists, but a calm no-fill interval releases half a
    # configured step so a wide quote cannot deadlock forever.
    state = strategy.state_dict()
    state.update(
        {
            "adaptive_extra_bps": 1.0,
            "adaptive_last_observation_wall": 1000.0,
            "adaptive_last_decay_wall": 1000.0,
            "adaptive_markouts": [[100.0, -0.01, 1000.0]],
        }
    )
    monkeypatch.setattr("entropy_bot.strategies.time.time", lambda: 1000.0)
    strategy.load_state(state)
    monkeypatch.setattr("entropy_bot.strategies.time.time", lambda: 2801.0)
    strategy.desired(market, AccountState())
    assert strategy.adaptive_extra_bps == 0.75


def test_effective_quote_components_are_exposed(tmp_path):
    cfg, market, fees = setup(tmp_path)
    cfg = replace(
        cfg,
        market_maker=replace(
            cfg.market_maker,
            vol_spread_mult=1.0,
            microprice_weight=0.0,
        ),
    )
    strategy = MarketMakerStrategy(cfg, fees)
    strategy._sigma_var_bps2 = 1.0
    strategy.desired(market, AccountState())
    metrics = strategy.adaptive_metrics
    assert metrics["effective_half_spread_bps"] == pytest.approx(
        max(
            cfg.market_maker.half_spread_bps,
            strategy.sigma_horizon_bps,
            fees.projected_net_maker_rate * 10_000
            + cfg.market_maker.min_edge_after_projected_rebate_bps / 2.0,
        )
    )
    assert metrics["per_side_ladder_budget_usd"] == cfg.market_maker.order_notional_usd
    assert metrics["microprice_weight"] == 0.0


def test_market_maker_adaptive_metrics_expose_rolling_decision_state(tmp_path):
    cfg, _market, fees = setup(tmp_path)
    cfg = replace(
        cfg,
        market_maker=replace(
            cfg.market_maker,
            adaptive_spread_enabled=True,
            adaptive_spread_min_samples=100,
            adaptive_spread_window_samples=200,
            adaptive_spread_adjust_every_samples=100,
        ),
    )
    strategy = MarketMakerStrategy(cfg, fees)
    row = {
        "horizon_sec": 30,
        "notional_usd": 100.0,
        "markout_pnl_usd": -0.01,
    }
    for _ in range(76):
        strategy.on_markout(row)

    metrics = strategy.adaptive_metrics
    assert metrics["window_samples"] == 76
    assert metrics["minimum_samples"] == 100
    assert metrics["samples_since_evaluation"] == 76
    assert metrics["samples_until_evaluation"] == 24
    assert metrics["weighted_markout_bps"] == pytest.approx(-1.0)
    assert metrics["projected_execution_wear_bps"] is not None


def test_live_stage_a_emits_two_valid_orders_at_realistic_sndk_price():
    cfg = load_config(
        Path(__file__).parents[1] / "config.live.stage-a.example.json"
    )
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
        [[{"px": "1462.7", "sz": "10"}], [{"px": "1462.8", "sz": "10"}]]
    )
    market = MarketSnapshot(meta, book, mark_px=1462.8, oracle_px=1462.4)
    fees = build_fee_model(meta, None, self_rebate_pct=160, referred_benefit_pct=0)

    intents = MarketMakerStrategy(cfg, fees).desired(market, AccountState())

    assert len(intents) == 2
    assert any(item.is_buy for item in intents)
    assert any(not item.is_buy for item in intents)
    assert all(item.notional >= cfg.risk.min_order_notional_usd for item in intents)
    assert sum(item.notional for item in intents) <= cfg.risk.max_open_order_usd


def test_grid_fill_blocks_and_favorable_move_rearms(tmp_path):
    cfg, market, fees = setup(tmp_path, "grid")
    s = GridStrategy(cfg, fees)
    first = s.desired(market, AccountState())
    buy = next(x for x in first if x.is_buy)
    s.on_fill(
        Fill(
            fill_id="x",
            order_role=buy.role,
            cloid="0x" + "0" * 32,
            oid=1,
            is_buy=True,
            price=buy.price,
            qty=buy.qty,
            fee_usd=0,
            liquidity="maker",
            time_ms=1,
        )
    )
    market.book.apply_hl(
        [
            [{"px": str(buy.price - 0.01), "sz": "10"}],
            [{"px": str(buy.price + 0.01), "sz": "10"}],
        ]
    )
    market.mark_px = market.oracle_px = buy.price
    blocked = {x.role for x in s.desired(market, AccountState())}
    assert buy.role not in blocked
    rearm_fair = buy.price * 1.002
    market.book.apply_hl(
        [
            [{"px": str(rearm_fair - 0.01), "sz": "10"}],
            [{"px": str(rearm_fair + 0.01), "sz": "10"}],
        ]
    )
    market.mark_px = market.oracle_px = rearm_fair
    rearmed = {x.role for x in s.desired(market, AccountState())}
    assert buy.role in rearmed
