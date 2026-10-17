import json
import secrets
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from aiohttp import web
from eth_account import Account

import dashboard
from dashboard_setup import save_wallet, wallet_status
from entropy_bot import credential_store
from entropy_bot.credentials import read_wallet_entries


@pytest.fixture(autouse=True)
def portable_wallet_storage_for_semantic_tests(monkeypatch):
    """Linux exercises save semantics with a deliberately test-only cipher.

    Real Windows DPAPI and unsupported-host refusal have dedicated tests.
    The opaque token maps to memory only and is not usable outside this test.
    """
    if sys.platform == "win32":
        return
    encrypted = {}

    def fake_dpapi(data, *, protect):
        if protect:
            token = secrets.token_bytes(32)
            encrypted[token] = data
            return token
        return encrypted[data]

    monkeypatch.setattr(credential_store, "_dpapi", fake_dpapi)


@pytest.mark.asyncio
async def test_unconfigured_monitor_repeated_refresh_has_same_safe_setup_message(monkeypatch):
    cfg = dashboard.load_config(Path(dashboard.__file__).parent / "config.example.json")
    monkeypatch.setattr(dashboard, "load_config", lambda _path: cfg)
    monkeypatch.setattr(dashboard, "resolve_live_credentials", lambda *args, **kwargs:
                        SimpleNamespace(account_address=None))
    monkeypatch.setattr(dashboard, "MarketClient", lambda *args, **kwargs:
                        pytest.fail("No market request is needed without a wallet"))
    monitor = dashboard.LiveMonitor(object())
    first = await monitor.snapshot()
    second = await monitor.snapshot()
    assert first["ok"] is False and second["ok"] is False
    assert first["code"] == second["code"] == "wallet_not_configured"
    assert first["error"] == second["error"]
    assert "尚未配置钱包" in second["error"] and "钱包与配置" in second["error"]
    assert "AssertionError" not in second["error"]
    assert monitor.cfg is monitor.market is monitor.address is None


@pytest.mark.asyncio
async def test_monitor_metadata_failure_leaves_initialization_retryable(monkeypatch):
    cfg = dashboard.load_config(Path(dashboard.__file__).parent / "config.example.json")
    monkeypatch.setattr(dashboard, "load_config", lambda _path: cfg)
    credentials = SimpleNamespace(account_address="0x" + "1" * 40,
                                  source="offline-fixture", account_was_derived=False)
    monkeypatch.setattr(dashboard, "resolve_live_credentials", lambda *args, **kwargs: credentials)
    attempts = []

    class Market:
        def __init__(self, *args, **kwargs):
            self.ready = False

        async def load_meta(self):
            attempts.append(self)
            if len(attempts) == 1:
                raise RuntimeError("synthetic-sensitive-response-must-not-be-echoed")
            self.ready = True

    monkeypatch.setattr(dashboard, "MarketClient", Market)
    monitor = dashboard.LiveMonitor(object(), "ANTH")
    failed = await monitor.snapshot()
    assert failed["ok"] is False
    assert "synthetic-sensitive" not in failed["error"]
    assert monitor.cfg is monitor.market is monitor.address is None
    assert monitor.credentials_source == "none"
    await monitor._initialize()
    assert len(attempts) == 2 and monitor.market is attempts[1]
    assert monitor.market.ready and monitor.cfg.market.symbol == "ANTH"
    assert monitor.address == credentials.account_address
    assert monitor.credentials_source == "offline-fixture"
    await monitor._initialize()
    assert len(attempts) == 2


def test_wallet_setup_masks_secret_and_preserves_other_wallets(tmp_path):
    path = tmp_path / "wallet.local.json"
    first, second = Account.create(), Account.create()
    account1, account2 = Account.create().address, Account.create().address
    save_wallet(path, {"wallet_id": "one", "private_key": first.key.hex(), "account_address": account1})
    save_wallet(path, {"wallet_id": "two", "private_key": second.key.hex(), "account_address": account2})
    save_wallet(path, {"wallet_id": "one", "private_key": "", "account_address": ""})
    rows = read_wallet_entries(path)
    assert rows[0].private_key == first.key.hex()
    assert rows[1].private_key == second.key.hex()
    assert first.key.hex() not in path.read_text()
    assert second.key.hex() not in path.read_text()
    public = json.dumps(wallet_status(path))
    assert first.key.hex() not in public and second.key.hex() not in public
    assert account1.lower() not in public
    assert all(row["storage_encrypted"] for row in wallet_status(path))


def test_setup_rejects_main_key_duplicates_and_invalid_input_without_overwrite(tmp_path):
    path = tmp_path / "wallet.local.json"
    api = Account.create()
    account = Account.create().address
    save_wallet(path, {"wallet_id": "one", "private_key": api.key.hex(), "account_address": account})
    before = path.read_bytes()
    for payload in [
        {"wallet_id": "two", "private_key": api.key.hex(), "account_address": account},
        {"wallet_id": "one", "private_key": api.key.hex(), "account_address": api.address},
        {"wallet_id": "one", "private_key": api.key.hex(), "account_address": Account.create().address},
        {"wallet_id": "../escape"}, {"wallet_id": False}, {"private_key": "bad", "account_address": account},
    ]:
        with pytest.raises(ValueError):
            save_wallet(path, payload)
        assert path.read_bytes() == before


@pytest.mark.parametrize("raw", [{"private_key": "", "account_address": ""},
                                 {"wallets": [{"id": "primary", "private_key": "", "account_address": ""}]}])
def test_empty_example_primary_does_not_become_second_active_wallet(tmp_path, raw):
    path = tmp_path / "wallet.local.json"
    path.write_text(json.dumps(raw))
    save_wallet(path, {"wallet_id": "one", "private_key": Account.create().key.hex(),
                       "account_address": Account.create().address})
    assert [e["id"] for e in wallet_status(path)] == ["one"]


def test_legacy_derived_wallet_duplicate_rejected_before_write(tmp_path):
    path = tmp_path / "wallet.local.json"
    funded = Account.create()
    path.write_text(json.dumps({"private_key": funded.key.hex(), "account_address": ""}))
    before = path.read_bytes()
    with pytest.raises(ValueError, match="资金账户已存在"):
        save_wallet(path, {"wallet_id": "new", "private_key": Account.create().key.hex(), "account_address": funded.address})
    assert path.read_bytes() == before


def test_dashboard_overrides_cannot_break_market_or_wallet_isolation(tmp_path, monkeypatch):
    config = tmp_path / "dashboard.config.json"
    monkeypatch.setattr(dashboard, "DASHBOARD_SETTINGS_FILE", config)
    for bad in [{"market": {"symbol": "SNDK"}}, {"runtime": {"state_dir": "state/shared"}}]:
        config.write_text(json.dumps({"defaults": bad}))
        with pytest.raises(ValueError, match="不能覆盖"):
            dashboard.PortfolioController({"one:SNDK": object(), "two:ANTH": object()})


def test_missing_local_budget_uses_shipped_small_example(tmp_path, monkeypatch):
    path = tmp_path / "dashboard.config.json"
    example = path.with_name("dashboard.config.example.json")
    example.write_bytes((Path(dashboard.__file__).parent / example.name).read_bytes())
    monkeypatch.setattr(dashboard, "DASHBOARD_SETTINGS_FILE", path)
    portfolio = dashboard.PortfolioController({symbol: object() for symbol in dashboard.LIVE_MARKETS})
    for bot in portfolio.controllers.values():
        profile = bot.runtime_overrides
        assert profile["market_maker"]["order_notional_usd"] == 12.0
        assert profile["risk"]["max_position_usd"] == 30.0
        assert profile["risk"]["max_open_order_usd"] == 30.0
        assert profile["risk"]["auto_scale_with_equity"] is False
        assert profile["risk"]["flatten_on_halt"] is False
        assert profile["fees"]["entropy_self_rebate_pct"] == 0.0
        assert profile["fees"]["entropy_referred_user_benefit_pct"] == 0.0


def test_missing_both_budget_files_refuses_legacy_live_fallback(tmp_path, monkeypatch):
    monkeypatch.setattr(dashboard, "DASHBOARD_SETTINGS_FILE", tmp_path / "dashboard.config.json")
    with pytest.raises(RuntimeError, match="面板预算配置缺失"):
        dashboard.PortfolioController({symbol: object() for symbol in dashboard.LIVE_MARKETS})


def test_explicit_local_budget_wins_and_invalid_local_is_not_ignored(tmp_path, monkeypatch):
    path = tmp_path / "dashboard.config.json"
    example = path.with_name("dashboard.config.example.json")
    example.write_bytes((Path(dashboard.__file__).parent / example.name).read_bytes())
    local = {"defaults": {"market_maker": {"order_notional_usd": 23.0}},
             "markets": {"ANTH": {"market_maker": {"order_notional_usd": 17.0}}}}
    path.write_text(json.dumps(local))
    monkeypatch.setattr(dashboard, "DASHBOARD_SETTINGS_FILE", path)
    portfolio = dashboard.PortfolioController({symbol: object() for symbol in dashboard.LIVE_MARKETS})
    assert portfolio.controllers["SNDK"].runtime_overrides["market_maker"]["order_notional_usd"] == 23.0
    assert portfolio.controllers["ANTH"].runtime_overrides["market_maker"]["order_notional_usd"] == 17.0
    path.write_text("{broken")
    with pytest.raises(json.JSONDecodeError):
        dashboard.PortfolioController({symbol: object() for symbol in dashboard.LIVE_MARKETS})


def test_read_only_preview_blocks_all_control_writes():
    request = SimpleNamespace(method="POST", app={"read_only": True, "control_token": "test"},
                              headers={"X-Entropy-Control": "test"})
    with pytest.raises(web.HTTPForbidden):
        dashboard._require_control_token(request)


@pytest.mark.asyncio
async def test_preview_starts_no_heartbeat_or_recovery_and_never_stops_controller(monkeypatch):
    class Session:
        async def close(self):
            self.closed = True
    session = Session()
    monkeypatch.setattr(dashboard, "build_rest_session", lambda: session)
    monkeypatch.setattr(dashboard, "configured_wallet_ids", lambda cfg: ["primary"])
    app = {"read_only": True}
    await dashboard.app_startup(app)
    assert "heartbeat_task" not in app and "recovery_task" not in app
    app["controller"].status = lambda: {"running": True}
    async def forbidden():
        pytest.fail("read-only preview must never stop a live controller")
    app["controller"].stop = forbidden
    await dashboard.app_cleanup(app)
    assert session.closed


@pytest.mark.asyncio
async def test_local_server_rejects_rebinding_and_cross_site_requests():
    async def handler(request):
        return "ok"
    for host, origin in [("attacker.example:8765", None), ("127.0.0.1:8765", "https://attacker.example")]:
        request = SimpleNamespace(host=host, headers={"Origin": origin} if origin else {})
        with pytest.raises(web.HTTPForbidden):
            await dashboard.local_origin_only(request, handler)
    assert await dashboard.local_origin_only(SimpleNamespace(host="127.0.0.1:8765", headers={}), handler) == "ok"
