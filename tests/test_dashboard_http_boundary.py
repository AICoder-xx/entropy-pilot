"""Real loopback HTTP checks with inert controllers, no credentials or orders."""
import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from dashboard_http import DashboardHTTP, PUBLIC_ASSETS
from entropy_bot.credential_store import CredentialStorageError

ROOT = Path(__file__).resolve().parents[1]


class InertController:
    def __init__(self):
        self.calls = []
        self._lock = asyncio.Lock()
        self.controllers = {}
        self.running = False

    def status(self):
        return {"running": self.running}

    def console_log_for(self, wallet_id, symbol):
        return f"synthetic-{wallet_id}-{symbol}.log"

    async def start(self, *args, **kwargs):
        self.calls.append(("start", args, kwargs))
        return {"accepted": True}

    async def stop(self):
        self.calls.append(("stop", (), {}))
        return {"accepted": True}

    async def start_market(self, *args, **kwargs):
        self.calls.append(("start_market", args, kwargs))
        return {"accepted": True}

    async def stop_market(self, *args):
        self.calls.append(("stop_market", args, {}))
        return {"accepted": True}

    def _market_controller(self, *args):
        self.calls.append(("market", args, {}))
        return self

    async def save_grid_settings(self, value):
        self.calls.append(("grid", (value,), {}))
        return value


@asynccontextmanager
async def http_client(*, read_only=False, services=None, monitors=None):
    controller = InertController()
    boundary = DashboardHTTP(lambda: services or SimpleNamespace(INDEX_FILE=ROOT / "dashboard/index.html"))
    @web.middleware
    async def local_only(request, handler):
        return await boundary.local_origin_only(request, handler)
    app = web.Application(middlewares=[local_only])
    app["control_token"] = "synthetic-control"
    app["read_only"] = read_only
    app["controller"] = controller
    if monitors is not None:
        app["monitors"] = monitors
    app.router.add_get("/", boundary.index)
    app.router.add_get("/assets/{name}", boundary.static_asset)
    app.router.add_route("*", "/setup", boundary.api_setup)
    app.router.add_get("/snapshot", boundary.api_snapshot)
    for path, method in ROUTES.items():
        app.router.add_post(path, getattr(boundary, method))
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        yield client, controller
    finally:
        await client.close()


ROUTES = {
    "/start": "api_start", "/stop": "api_stop", "/hold": "api_recovery_hold",
    "/market/start": "api_market_start", "/market/stop": "api_market_stop",
    "/market/hold": "api_market_recovery_hold", "/grid": "api_grid_settings",
}


@pytest.mark.asyncio
async def test_html_token_is_not_embedded_in_shared_script_and_all_assets_load():
    async with http_client() as (client, controller):
        response = await client.get("/")
        html = await response.text()
        assert response.status == 200
        assert 'content="synthetic-control"' in html
        assert "__CONTROL_TOKEN__" not in html
        assert response.headers["Cache-Control"] == "no-store"
        for name in PUBLIC_ASSETS:
            response = await client.get("/assets/" + name)
            assert response.status == 200
            assert "synthetic-control" not in await response.text()
            assert response.headers["X-Content-Type-Options"] == "nosniff"
        for path in ("/assets/wallet.local.json", "/assets/dashboard.py", "/assets/not-found.js"):
            assert (await client.get(path)).status == 404
        assert controller.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("headers", [{"Host": "attacker.example"}, {"Origin": "https://attacker.example"}])
async def test_remote_origin_and_rebound_host_cannot_read_token(headers):
    async with http_client() as (client, controller):
        response = await client.get("/", headers=headers)
        assert response.status == 403
        assert "synthetic-control" not in await response.text()
        assert not controller.calls


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ROUTES)
@pytest.mark.parametrize("read_only,token", [(True, "synthetic-control"), (False, "wrong"), (False, "")])
async def test_every_control_route_rejects_readonly_or_invalid_token(path, read_only, token):
    async with http_client(read_only=read_only) as (client, controller):
        response = await client.post(path, json={"strategy": "market_maker"}, headers={"X-Entropy-Control": token})
        assert response.status == 403
        assert not controller.calls


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ROUTES)
async def test_control_request_passes_only_selected_scope_to_controller(path):
    async with http_client() as (client, controller):
        payload = {"strategy": "grid", "wallet_id": "synthetic", "symbol": "SNDK", "grid": {"levels": 3}}
        response = await client.post(path, json=payload, headers={"X-Entropy-Control": "synthetic-control"})
        assert response.status == 200
        assert controller.calls
        name, args, kwargs = controller.calls[0]
        if path.startswith("/market") or path == "/grid":
            assert args[:2] == ("synthetic", "SNDK")
        if path.endswith("hold"):
            assert kwargs == {"recovery_hold_until_session_pause": True}
        if path == "/stop":
            assert name == "stop"


@pytest.mark.asyncio
@pytest.mark.parametrize("body", [[], None, "not an object"])
@pytest.mark.parametrize("path", [path for path in ROUTES if path != "/stop"])
async def test_nonobject_json_is_bad_request_without_controller_calls(path, body):
    async with http_client() as (client, controller):
        response = await client.post(path, json=body, headers={"X-Entropy-Control": "synthetic-control"})
        assert response.status == 400
        assert not controller.calls


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["GET", "POST"])
async def test_unreadable_encrypted_wallet_returns_actionable_conflict(method, tmp_path):
    wallet = tmp_path / "wallet.local.json"
    original = b'{"format":"synthetic-ciphertext-envelope"}'
    wallet.write_bytes(original)
    def unavailable(*args):
        raise CredentialStorageError("原密文保持不变，请在原 Windows 用户恢复读取")
    services = SimpleNamespace(
        LIVE_TEMPLATE=tmp_path / "template.json",
        load_config=lambda path: SimpleNamespace(live=SimpleNamespace(credentials_file=wallet)),
        save_wallet=unavailable, wallet_status=unavailable,
    )
    async with http_client(services=services) as (client, controller):
        response = await client.request(method, "/setup", json={} if method == "POST" else None,
                                        headers={"X-Entropy-Control": "synthetic-control"})
        assert response.status == 409
        assert (await response.json())["ok"] is False
        assert "原密文" in (await response.json())["error"]
        assert response.headers["Cache-Control"] == "no-store"
        assert wallet.read_bytes() == original
        assert not controller.calls


@pytest.mark.asyncio
@pytest.mark.parametrize("running", [False, True])
async def test_snapshot_never_competes_with_running_trader_for_official_reads(running):
    calls = []
    class Monitor:
        symbol = "SNDK"
        async def snapshot(self):
            calls.append("official-read")
            return {"source": "official"}
    summary = {"coin": "io:SNDK", "wallet_id": "synthetic"}
    services = SimpleNamespace(
        build_local_snapshot=lambda profile: {"summary": summary, "profile": profile},
        live_from_bot_summary=lambda value: {"source": "local", "summary": value},
        read_log_tail=lambda path: "synthetic log",
    )
    async with http_client(services=services, monitors={"synthetic:SNDK": Monitor()}) as (client, controller):
        controller.running = running
        response = await client.get("/snapshot?profile=synthetic")
        assert response.status == 200
        payload = await response.json()
        assert payload["live"]["source"] == ("local" if running else "official")
        assert calls == ([] if running else ["official-read"])
        assert payload["profile"] == "synthetic"
