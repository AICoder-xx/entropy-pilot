import asyncio
import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from aiohttp import web

import dashboard


@pytest.fixture
def budget_app(tmp_path, monkeypatch):
    root = Path(dashboard.__file__).parent
    template = tmp_path / "template.json"
    template.write_bytes((root / "config.example.json").read_bytes())
    settings = tmp_path / "dashboard.config.json"
    settings.write_bytes((root / "dashboard.config.example.json").read_bytes())
    for name, value in {
        "ROOT": tmp_path, "LIVE_TEMPLATE": template, "DASHBOARD_SETTINGS_FILE": settings,
        "TUNING_FILE": tmp_path / "tuning.overrides.json", "STATE_DIR": tmp_path / "state",
        "LOG_DIR": tmp_path / "logs", "CONTROL_FILE": tmp_path / "state/ui_control.json",
        "GRID_SETTINGS_FILE": tmp_path / "grid.settings.json",
    }.items():
        monkeypatch.setattr(dashboard, name, value)
    monkeypatch.setattr(dashboard, "resolve_live_credentials", lambda *a, **k:
                        pytest.fail("Budget editing must not read credentials"))
    monitors = {f"{wallet}:{symbol}": object()
                for wallet in ("one", "two") for symbol in dashboard.LIVE_MARKETS}
    return {"controller": dashboard.PortfolioController(monitors), "monitors": monitors,
            "control_token": "test-budget", "read_only": False}


def request(app, payload=None, *, method="POST", during_body=None):
    async def body():
        if during_body:
            during_body()
        await asyncio.sleep(0)
        return payload
    return SimpleNamespace(method=method, app=app, json=body,
                           headers={"X-Entropy-Control": "test-budget"})


def edit_payload(**values):
    return {"symbol": "SNDK", "fixed_budget": True, "values": {
        "order_notional_usd": 15.0, "max_position_usd": 40.0,
        "max_open_order_usd": 40.0, "max_drawdown_usd": 4.0, **values}}


@pytest.mark.asyncio
async def test_get_shows_next_budget_without_reading_wallet_or_mutating_files(budget_app):
    before = dashboard.DASHBOARD_SETTINGS_FILE.read_bytes()
    response = await dashboard.api_budget(request(budget_app, method="GET"))
    state = json.loads(response.text)
    assert response.status == 200
    assert state["markets"]["SNDK"]["values"]["order_notional_usd"] == 12
    assert state["markets"]["SNDK"]["wallet_ids"] == ["one", "two"]
    assert state["markets"]["SNDK"]["editable"] is True
    assert state["markets"]["SNDK"]["effective"]["at_capital"] == state["markets"]["SNDK"]["values"]
    assert dashboard.DASHBOARD_SETTINGS_FILE.read_bytes() == before
    assert not dashboard.STATE_DIR.exists()


@pytest.mark.asyncio
async def test_save_fixed_budget_preserves_other_fields_and_live_controller_identity(budget_app):
    settings = json.loads(dashboard.DASHBOARD_SETTINGS_FILE.read_text())
    settings["defaults"]["risk"]["auto_scale_with_equity"] = True
    settings["defaults"]["risk"]["auto_scale_max_equity_usd"] = 200.0
    dashboard.DASHBOARD_SETTINGS_FILE.write_text(json.dumps(settings))
    app = budget_app
    app["controller"] = dashboard.PortfolioController(app["monitors"])
    controller = app["controller"]
    for bot in controller.controllers.values():
        bot._recovery_attempts_ms.append(int(dashboard.time.time() * 1000))
    histories = {key: list(bot._recovery_attempts_ms) for key, bot in controller.controllers.items()}
    identities = {key: (id(bot), id(bot._lock))
                  for key, bot in controller.controllers.items()}
    other = {key: copy.deepcopy(bot.runtime_overrides) for key, bot in controller.controllers.items()
             if bot.symbol == "ANTH"}
    before = dashboard.budget_state(app)["markets"]["SNDK"]
    assert before["auto_scale_with_equity"] is True
    assert before["effective"]["at_max_equity"]["max_position_usd"] > before["values"]["max_position_usd"]
    response = await dashboard.api_budget(request(app, edit_payload()))
    assert response.status == 200, response.text
    state = json.loads(response.text)
    assert state["saved_symbol"] == "SNDK"
    assert state["markets"]["SNDK"]["values"] == edit_payload()["values"]
    assert state["markets"]["SNDK"]["auto_scale_with_equity"] is False
    assert state["markets"]["SNDK"]["effective"]["at_max_equity"] == edit_payload()["values"]
    saved = json.loads(dashboard.DASHBOARD_SETTINGS_FILE.read_text())
    expected = copy.deepcopy(settings)
    expected["markets"]["SNDK"]["market_maker"] = {"order_notional_usd": 15.0}
    expected["markets"]["SNDK"]["risk"] = {
        "max_position_usd": 40.0, "max_open_order_usd": 40.0,
        "max_session_drawdown_usd": 4.0, "auto_scale_with_equity": False}
    assert saved == expected
    for key, bot in controller.controllers.items():
        assert identities[key] == (id(bot), id(bot._lock))
        assert histories[key] == list(bot._recovery_attempts_ms)
        if bot.symbol == "ANTH":
            assert bot.runtime_overrides == other[key]
        else:
            assert bot.runtime_overrides["risk"]["auto_scale_with_equity"] is False
    reloaded = dashboard.PortfolioController(app["monitors"])
    assert reloaded.controllers["one:SNDK"].runtime_overrides == controller.controllers["one:SNDK"].runtime_overrides


@pytest.mark.parametrize("bad", [None, True, "12", 0, -1, float("nan"), float("inf"), 10 ** 500])
@pytest.mark.asyncio
async def test_invalid_budget_numbers_do_not_write(budget_app, bad):
    before = dashboard.DASHBOARD_SETTINGS_FILE.read_bytes()
    response = await dashboard.api_budget(request(budget_app, edit_payload(order_notional_usd=bad)))
    assert response.status == 400
    assert dashboard.DASHBOARD_SETTINGS_FILE.read_bytes() == before


@pytest.mark.parametrize("values", [
    {"order_notional_usd": 10}, {"max_position_usd": 14},
    {"max_position_usd": 101}, {"max_open_order_usd": 29},
])
@pytest.mark.asyncio
async def test_invalid_budget_relations_do_not_write(budget_app, values):
    before = dashboard.DASHBOARD_SETTINGS_FILE.read_bytes()
    response = await dashboard.api_budget(request(budget_app, edit_payload(**values)))
    assert response.status == 400
    assert dashboard.DASHBOARD_SETTINGS_FILE.read_bytes() == before


@pytest.mark.parametrize("kind", ["running", "recovering", "hard_recovering", "desired_running", "halted"])
@pytest.mark.asyncio
async def test_save_rechecks_running_and_recovery_after_body_read(budget_app, kind):
    before = dashboard.DASHBOARD_SETTINGS_FILE.read_bytes()
    controller = budget_app["controller"]
    bot = controller.controllers["one:SNDK"]
    def race():
        if kind == "running":
            controller.status = lambda: {"running": True}
        elif kind == "recovering":
            bot._recovering = True
        elif kind == "hard_recovering":
            bot._hard_recovering = True
        elif kind == "desired_running":
            bot.status = lambda: {"running": False, "desired_state": "running"}
        else:
            bot.status = lambda: {"running": False, "desired_state": "halted"}
    response = await dashboard.api_budget(request(budget_app, edit_payload(), during_body=race))
    assert response.status == 409
    assert dashboard.DASHBOARD_SETTINGS_FILE.read_bytes() == before


@pytest.mark.asyncio
async def test_budget_does_not_add_drawdown_limit_beyond_existing_config_parser(budget_app):
    response = await dashboard.api_budget(request(budget_app, edit_payload(max_drawdown_usd=150)))
    assert response.status == 200, response.text
    assert json.loads(response.text)["markets"]["SNDK"]["values"]["max_drawdown_usd"] == 150


@pytest.mark.asyncio
async def test_budget_token_and_read_only_guards(budget_app):
    req = request(budget_app, edit_payload())
    req.headers = {}
    with pytest.raises(web.HTTPForbidden):
        await dashboard.api_budget(req)
    budget_app["read_only"] = True
    with pytest.raises(web.HTTPForbidden):
        await dashboard.api_budget(request(budget_app, edit_payload()))
    state = json.loads((await dashboard.api_budget(request(budget_app, method="GET"))).text)
    assert state["markets"]["SNDK"]["editable"] is False


@pytest.mark.asyncio
async def test_tuning_budget_collision_is_visible_and_blocks_save(budget_app, monkeypatch):
    before = dashboard.DASHBOARD_SETTINGS_FILE.read_bytes()
    monkeypatch.setitem(dashboard.TUNING_BOUNDS, "market_maker.order_notional_usd", (11, 100))
    dashboard.TUNING_FILE.write_text(json.dumps({"SNDK": {"market_maker.order_notional_usd": 22}}))
    state = dashboard.budget_state(budget_app)
    assert state["markets"]["SNDK"]["editable"] is False
    assert "冲突" in state["markets"]["SNDK"]["reason"]
    response = await dashboard.api_budget(request(budget_app, edit_payload()))
    assert response.status == 409
    assert dashboard.DASHBOARD_SETTINGS_FILE.read_bytes() == before


@pytest.mark.asyncio
async def test_shared_budget_accounts_for_each_wallet_saved_grid_size(budget_app):
    dashboard.GRID_SETTINGS_FILE.write_text(json.dumps({"two:SNDK": {"order_notional_usd": 25}}))
    before = dashboard.DASHBOARD_SETTINGS_FILE.read_bytes()
    grid_before = dashboard.GRID_SETTINGS_FILE.read_bytes()
    state = dashboard.budget_state(budget_app)
    assert state["markets"]["SNDK"]["limits"]["grid_order_notional_usd"] == 25
    response = await dashboard.api_budget(request(budget_app, edit_payload(max_open_order_usd=40)))
    assert response.status == 400
    assert dashboard.DASHBOARD_SETTINGS_FILE.read_bytes() == before
    response = await dashboard.api_budget(request(budget_app, edit_payload(max_open_order_usd=50)))
    assert response.status == 200, response.text
    assert dashboard.GRID_SETTINGS_FILE.read_bytes() == grid_before


@pytest.mark.asyncio
async def test_effective_crisis_quote_cannot_exceed_saved_fixed_side_budget(budget_app):
    dashboard.TUNING_FILE.write_text(json.dumps({"SNDK": {"market_maker.crisis_quote_notional_usd": 800}}))
    before = dashboard.DASHBOARD_SETTINGS_FILE.read_bytes()
    assert dashboard.budget_state(budget_app)["markets"]["SNDK"]["limits"]["crisis_quote_notional_usd"] == 800
    response = await dashboard.api_budget(request(budget_app, edit_payload()))
    assert response.status == 400 and "危机" in json.loads(response.text)["error"]
    assert dashboard.DASHBOARD_SETTINGS_FILE.read_bytes() == before
    dashboard.TUNING_FILE.write_text(json.dumps({"SNDK": {"market_maker.crisis_quote_notional_usd": 15}}))
    response = await dashboard.api_budget(request(budget_app, edit_payload()))
    assert response.status == 400
    assert dashboard.DASHBOARD_SETTINGS_FILE.read_bytes() == before
    dashboard.TUNING_FILE.write_text(json.dumps({"SNDK": {"market_maker.crisis_quote_notional_usd": 7.5}}))
    response = await dashboard.api_budget(request(budget_app, edit_payload()))
    assert response.status == 200, response.text


@pytest.mark.asyncio
async def test_full_config_validation_and_write_failure_leave_config_unchanged(budget_app, monkeypatch):
    before = dashboard.DASHBOARD_SETTINGS_FILE.read_bytes()
    template = json.loads(dashboard.LIVE_TEMPLATE.read_text(encoding="utf-8"))
    template["runtime"]["loop_interval_sec"] = -1
    dashboard.LIVE_TEMPLATE.write_text(json.dumps(template))
    response = await dashboard.api_budget(request(budget_app, edit_payload()))
    assert response.status == 400
    assert dashboard.DASHBOARD_SETTINGS_FILE.read_bytes() == before
    template["runtime"]["loop_interval_sec"] = 0.5
    dashboard.LIVE_TEMPLATE.write_text(json.dumps(template))
    old = copy.deepcopy(budget_app["controller"].controllers["one:SNDK"].runtime_overrides)
    def failed_write(*args):
        raise PermissionError("synthetic-sensitive-path")
    monkeypatch.setattr(dashboard, "atomic_write_json", failed_write)
    response = await dashboard.api_budget(request(budget_app, edit_payload()))
    assert response.status == 500 and "synthetic-sensitive" not in response.text
    assert dashboard.DASHBOARD_SETTINGS_FILE.read_bytes() == before
    assert budget_app["controller"].controllers["one:SNDK"].runtime_overrides == old
