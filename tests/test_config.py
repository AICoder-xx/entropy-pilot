import json
from pathlib import Path

import pytest

from entropy_bot.config import ConfigError, load_config


EXAMPLE = Path(__file__).parents[1] / "config.example.json"


@pytest.mark.parametrize("section,key,value", [
    ("risk", "max_oracle_deviation_bps", float("nan")),
    ("risk", "min_free_collateral_usd", float("inf")),
    ("risk", "auto_scale_max_equity_usd", float("nan")),
    ("risk", "max_consecutive_errors", True),
    ("risk", "max_consecutive_errors", 0),
    ("risk", "max_book_spread_bps", -1),
    ("market_maker", "levels", True),
    ("market_maker", "adaptive_spread_min_samples", "100"),
    ("market_maker", "adaptive_spread_min_samples", 100.5),
    ("market_maker", "order_notional_usd", float("nan")),
    ("market_maker", "half_spread_bps", -1),
    ("runtime", "budget_soft_remaining", True),
    ("runtime", "budget_hard_remaining", -1),
    ("market", "max_book_age_sec", 0),
    ("paper", "initial_equity_usd", -1),
])
def test_unsafe_numeric_config_is_rejected(tmp_path, section, key, value):
    raw = json.loads(EXAMPLE.read_text(encoding="utf-8"))
    raw[section][key] = value
    path = tmp_path / "config.json"
    path.write_text(json.dumps(raw), encoding="utf-8")

    with pytest.raises(ConfigError):
        load_config(path)


def test_new_user_example_is_paper_without_rebate_or_paid_actions():
    cfg = load_config(EXAMPLE)

    assert cfg.mode == "paper"
    assert cfg.fees.entropy_self_rebate_pct == 0
    assert cfg.fees.entropy_referred_user_benefit_pct == 0
    assert not cfg.runtime.auto_reserve_actions
    assert cfg.runtime.reserve_actions_daily_max_usd == 0
    assert not cfg.risk.auto_scale_with_equity
    assert cfg.live.env_file == EXAMPLE.parent / ".env"
    assert cfg.live.private_key_env == "HL_API_PRIVATE_KEY"


@pytest.mark.parametrize("env_file", [False, "", [], 123])
def test_invalid_dotenv_path_is_rejected(tmp_path, env_file):
    raw = json.loads(EXAMPLE.read_text(encoding="utf-8"))
    raw["live"]["env_file"] = env_file
    path = tmp_path / "config.json"
    path.write_text(json.dumps(raw), encoding="utf-8")

    with pytest.raises(ConfigError, match="live.env_file"):
        load_config(path)
