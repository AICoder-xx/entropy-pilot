from datetime import datetime
import json
from pathlib import Path

import dashboard
from entropy_bot.config import load_config
from entropy_bot.trading_schedule import NEW_YORK, us_regular_session_state


def eastern(year: int, month: int, day: int, hour: int, minute: int) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=NEW_YORK)


def test_regular_session_gate_uses_five_minute_buffers():
    assert not us_regular_session_state(
        enabled=True, now=eastern(2026, 8, 28, 9, 24)
    ).paused
    assert us_regular_session_state(
        enabled=True, now=eastern(2026, 8, 28, 9, 25)
    ).paused
    assert us_regular_session_state(
        enabled=True, now=eastern(2026, 8, 28, 16, 4)
    ).paused
    assert not us_regular_session_state(
        enabled=True, now=eastern(2026, 8, 28, 16, 5)
    ).paused


def test_sixty_minute_buffer_pauses_at_0830_eastern():
    assert not us_regular_session_state(
        enabled=True,
        now=eastern(2026, 8, 28, 8, 29),
        pre_open_buffer_min=60,
    ).paused
    assert us_regular_session_state(
        enabled=True,
        now=eastern(2026, 8, 28, 8, 30),
        pre_open_buffer_min=60,
    ).paused


def test_weekends_and_full_day_holidays_remain_enabled():
    assert not us_regular_session_state(
        enabled=True, now=eastern(2026, 8, 29, 12, 0)
    ).paused
    assert not us_regular_session_state(
        enabled=True, now=eastern(2026, 9, 7, 12, 0)
    ).paused


def test_published_early_close_resumes_after_1305_eastern():
    assert us_regular_session_state(
        enabled=True, now=eastern(2026, 11, 27, 13, 4)
    ).paused
    assert not us_regular_session_state(
        enabled=True, now=eastern(2026, 11, 27, 13, 5)
    ).paused


def test_2028_saturday_new_year_has_no_friday_observance():
    # NYSE's published 2028 calendar explicitly says no New Year's holiday is
    # observed when Jan 1 falls on Saturday.
    assert us_regular_session_state(
        enabled=True, now=eastern(2027, 12, 31, 12, 0)
    ).paused


def test_live_runtime_regular_session_gate_configuration(tmp_path, monkeypatch):
    root = Path(__file__).parents[1]
    settings = tmp_path / "dashboard.config.json"
    settings.with_name("dashboard.config.example.json").write_bytes(
        (root / "dashboard.config.example.json").read_bytes()
    )
    monkeypatch.setattr(dashboard, "DASHBOARD_SETTINGS_FILE", settings)
    portfolio = dashboard.PortfolioController(
        {symbol: object() for symbol in dashboard.LIVE_MARKETS}
    )
    template = json.loads((root / "config.example.json").read_text(encoding="utf-8"))
    # The listed SNDK perp uses the cash-session pickoff guard.  ANTH is a
    # pre-IPO market and stays eligible while its volatility guards remain on.
    for symbol, pause in {"SNDK": True, "ANTH": False}.items():
        payload = json.loads(json.dumps(template))
        dashboard._merge_dict(payload, portfolio.controllers[symbol].runtime_overrides)
        generated = tmp_path / f"config.live.ui.{symbol.lower()}.runtime.json"
        generated.write_text(json.dumps(payload), encoding="utf-8")
        cfg = load_config(generated)
        assert cfg.market.symbol == symbol
        assert cfg.runtime.pause_during_us_regular_session is pause
        assert cfg.runtime.us_regular_session_pre_open_buffer_min == 30
        assert cfg.runtime.us_regular_session_post_close_buffer_min == 5
