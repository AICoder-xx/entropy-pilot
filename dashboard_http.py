"""Local HTTP boundary: request validation, control APIs and public assets.

Controller construction and disk access are injected. The web layer cannot
choose an account or submit an order independently of the controller gates.
"""
from __future__ import annotations

import json
import os
import secrets
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, TYPE_CHECKING

from aiohttp import web
from entropy_bot.credential_store import CredentialStorageError

from dashboard_budget import (
    BudgetValidationError, budget_preview, parse_budget_request,
    tuning_budget_conflict, validate_budget_relations,
    validate_complete_payload, values_from_config, with_fixed_budget,
)

if TYPE_CHECKING:
    from dashboard import BotController, PortfolioController


PUBLIC_ASSETS = frozenset({
    "common.js", "strategy.js", "metrics.js", "charts.js", "dashboard.js",
    "budget.js", "wallet.js", "events.js", "guide.js", "dashboard.css", "guide.css",
})


@dataclass(frozen=True)
class DashboardServices:
    INDEX_FILE: Path
    LIVE_TEMPLATE: Path
    DASHBOARD_SETTINGS_FILE: Path
    LIVE_MARKETS: tuple[str, ...]
    build_local_snapshot: Callable
    live_from_bot_summary: Callable
    read_log_tail: Callable
    load_config: Callable
    save_wallet: Callable
    wallet_status: Callable
    configured_wallet_ids: Callable
    LiveMonitor: Callable
    PortfolioController: Callable
    _merge_dict: Callable
    apply_tuning_overrides: Callable
    load_tuning_overrides: Callable
    load_dashboard_settings: Callable
    atomic_write_json: Callable


class DashboardHTTP:
    def __init__(self, services: Callable[[], DashboardServices]) -> None:
        self._services = services

    @staticmethod
    async def _read_object(request: web.Request) -> dict:
        try:
            payload = await request.json()
        except (ValueError, TypeError, UnicodeError):
            raise web.HTTPBadRequest(text="请求正文必须是 JSON 对象") from None
        if not isinstance(payload, dict):
            raise web.HTTPBadRequest(text="请求正文必须是 JSON 对象")
        return payload

    async def static_asset(self, request: web.Request) -> web.StreamResponse:
        name = request.match_info["name"]
        if name not in PUBLIC_ASSETS:
            raise web.HTTPNotFound()
        root = (self._services().INDEX_FILE.parent / "assets").resolve()
        path = (root / name).resolve()
        if path.parent != root or not path.is_file():
            raise web.HTTPNotFound()
        return web.FileResponse(path, headers={
            "Cache-Control": "no-cache",
            "X-Content-Type-Options": "nosniff",
        })

    async def index(self, request: web.Request) -> web.Response:
        deps = self._services()
        html = deps.INDEX_FILE.read_text(encoding="utf-8")
        html = html.replace("__CONTROL_TOKEN__", request.app["control_token"])
        return web.Response(
            text=html,
            content_type="text/html",
            headers={
                "Cache-Control": "no-store",
                "X-Content-Type-Options": "nosniff",
                "X-Frame-Options": "DENY",
                "Content-Security-Policy": (
                    "default-src 'self'; style-src 'self' 'unsafe-inline'; "
                    "script-src 'self'; connect-src 'self'; "
                    "img-src 'self' data:; frame-ancestors 'none'"
                ),
            },
        )


    async def api_snapshot(self, request: web.Request) -> web.Response:
        deps = self._services()
        local = deps.build_local_snapshot(request.query.get("profile"))
        summary = local.get("summary") or {}
        coin = str(summary.get("coin") or "io:SNDK")
        symbol = coin.split(":")[-1].upper()
        wallet_id = str(summary.get("wallet_id") or "primary")
        monitors: dict[str, deps.LiveMonitor] = request.app["monitors"]
        monitor_key = f"{wallet_id}:{symbol}"
        monitor = monitors.get(monitor_key) or next(iter(monitors.values()))
        portfolio: deps.PortfolioController = request.app["controller"]
        controller_status = portfolio.status()
        if controller_status.get("running"):
            # Never let a browser refresh compete with a live trader, even when
            # the selected profile is a stale pre-migration alias. Show local
            # telemetry age/staleness instead of falling back to official REST.
            live = deps.live_from_bot_summary(summary)
        else:
            live = await monitor.snapshot()
        console_log = portfolio.console_log_for(wallet_id, monitor.symbol)
        value = {
            "ok": True,
            "read_only": bool(request.app.get("read_only")),
            **local,
            "live": live,
            "controller": controller_status,
            "bot_log": deps.read_log_tail(console_log),
        }
        return web.json_response(value, headers={"Cache-Control": "no-store"})


    def _require_control_token(self, request: web.Request) -> None:
        if request.method != "GET" and request.app.get("read_only"):
            raise web.HTTPForbidden(text="只读预览不允许保存配置或交易控制")
        if not secrets.compare_digest(
            request.headers.get("X-Entropy-Control", ""),
            request.app["control_token"],
        ):
            raise web.HTTPForbidden(text="控制令牌无效")


    async def api_setup(self, request: web.Request) -> web.Response:
        deps = self._services()
        self._require_control_token(request)
        cfg = deps.load_config(deps.LIVE_TEMPLATE)
        controller = request.app["controller"]
        if request.method == "POST":
            # Read the potentially slow request body before testing recovery state.
            payload = await self._read_object(request)
            async with controller._lock:
                if controller.status().get("running") or any(
                    bot._recovering or bot._hard_recovering or bot.status().get("desired_state") == "running"
                    for bot in controller.controllers.values()
                ):
                    raise web.HTTPConflict(text="请先安全停止全部策略，再修改钱包配置")
                try:
                    deps.save_wallet(cfg.live.credentials_file, payload)
                except CredentialStorageError as exc:
                    return web.json_response({"ok": False, "error": str(exc)}, status=409,
                                             headers={"Cache-Control": "no-store"})
                except (ValueError, RuntimeError, json.JSONDecodeError) as exc:
                    # Helpers only raise fixed labels, never include submitted secrets.
                    raise web.HTTPBadRequest(text=str(exc)) from None
                wallet_ids = deps.configured_wallet_ids(cfg) or ["primary"]
                monitors = {f"{wallet_id}:{symbol}": deps.LiveMonitor(request.app["session"], symbol, wallet_id)
                            for wallet_id in wallet_ids for symbol in deps.LIVE_MARKETS}
                refreshed = deps.PortfolioController(monitors)
                controller.controllers = refreshed.controllers
                controller.wallet_ids = refreshed.wallet_ids
                request.app["monitors"].clear()
                request.app["monitors"].update(monitors)
        try:
            wallets = deps.wallet_status(cfg.live.credentials_file)
        except CredentialStorageError as exc:
            return web.json_response({"ok": False, "error": str(exc)}, status=409,
                                     headers={"Cache-Control": "no-store"})
        storage_note = (
            "新保存的钱包私钥使用 Windows DPAPI 保护，仅当前 Windows 用户可解密。"
            if os.name == "nt" else "当前系统不支持安全保存钱包；请在受支持的 Windows 环境保存配置。"
        )
        if any(not wallet.get("storage_encrypted") for wallet in wallets):
            storage_note += " 现有明文钱包未自动迁移；在面板重新保存后才启用加密保管。"
        return web.json_response({"ok": True, "wallets": wallets,
                                  "running": bool(controller.status().get("running")),
                                  "credential_note": storage_note + (" " +
                                      "primary 的进程环境变量或显式 .env 优先于本机钱包文件；请使用命名钱包隔离不同账户。"
                                      if os.environ.get(cfg.live.private_key_env) or os.environ.get(cfg.live.account_address_env)
                                      or (cfg.live.env_file and cfg.live.env_file.exists()) else ""
                                  ),
                                  "read_only": bool(request.app.get("read_only"))},
                                 headers={"Cache-Control": "no-store"})


    def _budget_edit_reason(self, controller: PortfolioController, *, read_only: bool = False) -> str:
        if read_only:
            return "只读预览不能保存预算"
        if controller.status().get("running") or any(
            bot._recovering or bot._hard_recovering or bot.status().get("desired_state") != "stopped"
            for bot in controller.controllers.values()
        ):
            return "请先安全停止全部策略及自动恢复，再保存固定预算"
        return ""


    def _next_budget_payload(self, bot: BotController, template: dict) -> dict:
        deps = self._services()
        payload = json.loads(json.dumps(template))
        payload["strategy"] = "market_maker"
        deps._merge_dict(payload, bot.runtime_overrides)
        # Grid has a separate per-wallet settings overlay at actual start. Budget
        # validation must include it even though this editor changes only MM size.
        payload.setdefault("grid", {}).update(bot._validate_grid_settings({}))
        deps.apply_tuning_overrides(payload, bot.symbol)
        return payload


    def budget_state(self, app: web.Application, *, saved_symbol: str | None = None) -> dict:
        deps = self._services()
        controller: deps.PortfolioController = app["controller"]
        read_only = bool(app.get("read_only"))
        reason = self._budget_edit_reason(controller, read_only=read_only)
        base = deps.load_config(deps.LIVE_TEMPLATE)
        template = json.loads(deps.LIVE_TEMPLATE.read_text(encoding="utf-8"))
        result = {}
        for symbol in deps.LIVE_MARKETS:
            bots = [bot for bot in controller.controllers.values() if bot.symbol == symbol]
            if not bots:
                continue
            payload = self._next_budget_payload(bots[0], template)
            cfg, effective = budget_preview(base, payload)
            tuning = payload.get("_tuning_applied") or {}
            market_reason = reason or (
                "调参覆盖与预算字段冲突，请先移除该市场对应的调参字段"
                if tuning_budget_conflict(tuning) else ""
            )
            result[symbol] = {
                "values": values_from_config(cfg),
                "editable": not market_reason,
                "reason": market_reason,
                "wallet_ids": [bot.wallet_id for bot in bots],
                "auto_scale_with_equity": cfg.risk.auto_scale_with_equity,
                "limits": {"min_order_notional_usd": cfg.risk.min_order_notional_usd,
                           "capital_usd": cfg.risk.capital_usd,
                           "crisis_quote_notional_usd": cfg.market_maker.crisis_quote_notional_usd,
                           "grid_order_notional_usd": max(
                               bot._validate_grid_settings({})["order_notional_usd"] for bot in bots)},
                "effective": effective,
                "tuning_applied": tuning,
                "tuning_rejected": payload.get("_tuning_rejected") or {},
                "next_runtime_overrides": {section: payload[section]
                                           for section in ("risk", "market_maker", "grid", "fees")},
            }
        return {"ok": True, "read_only": read_only,
                "running": bool(controller.status().get("running")),
                "scope": "all_enabled_wallets_for_market", "saved_symbol": saved_symbol,
                "markets": result}


    async def api_budget(self, request: web.Request) -> web.Response:
        deps = self._services()
        self._require_control_token(request)
        saved_symbol = None
        try:
            if request.method == "POST":
                # A slow body cannot hold a stale stopped-state decision.
                try:
                    payload = await self._read_object(request)
                except (ValueError, TypeError):
                    raise BudgetValidationError("预算请求不是有效 JSON") from None
                symbol, values = parse_budget_request(payload, deps.LIVE_MARKETS)
                controller: deps.PortfolioController = request.app["controller"]
                async with controller._lock:
                    reason = self._budget_edit_reason(controller)
                    if reason:
                        return web.json_response({"ok": False, "error": reason}, status=409)
                    tuning, _ = deps.load_tuning_overrides(symbol)
                    if tuning_budget_conflict(tuning):
                        return web.json_response({"ok": False, "error":
                            "调参覆盖与预算字段冲突，预算未保存；请先移除该市场对应的调参字段"}, status=409)
                    candidate = with_fixed_budget(deps.load_dashboard_settings(), symbol, values)
                    prepared = deps.PortfolioController(request.app["monitors"], settings=candidate)
                    template = json.loads(deps.LIVE_TEMPLATE.read_text(encoding="utf-8"))
                    base = deps.load_config(deps.LIVE_TEMPLATE)
                    updates = {}
                    for key, bot in prepared.controllers.items():
                        if bot.symbol != symbol:
                            continue
                        next_payload = self._next_budget_payload(bot, template)
                        cfg, _ = budget_preview(base, next_payload)
                        if tuning_budget_conflict(next_payload.get("_tuning_applied") or {}):
                            return web.json_response({"ok": False, "error":
                                "调参覆盖与预算字段冲突，预算未保存；请先移除该市场对应的调参字段"}, status=409)
                        if values_from_config(cfg) != values or cfg.risk.auto_scale_with_equity:
                            raise BudgetValidationError("实际生效预算与输入不一致，预算未保存")
                        validate_budget_relations(values, cfg)
                        validate_complete_payload(next_payload)
                        updates[key] = bot.runtime_overrides
                    if not updates or any(key not in controller.controllers for key in updates):
                        raise BudgetValidationError("市场控制器已变化，请刷新页面后重试")
                    # Prepare/serialize the response before the commit as well, so
                    # a bad untouched profile cannot turn a successful save into an
                    # apparent validation failure after the file already changed.
                    prepared.controllers = {key: (prepared.controllers[key] if key in updates else bot)
                                            for key, bot in controller.controllers.items()}
                    staged_app = dict(request.app)
                    staged_app["controller"] = prepared
                    response = web.json_response(self.budget_state(staged_app, saved_symbol=symbol),
                                                 headers={"Cache-Control": "no-store"})
                    # No await from the final stopped-state check through fsync and
                    # replacement: recovery cannot interleave a start with this save.
                    deps.atomic_write_json(deps.DASHBOARD_SETTINGS_FILE, candidate)
                    for key, overrides in updates.items():
                        controller.controllers[key].runtime_overrides = overrides
                    return response
            value = self.budget_state(request.app, saved_symbol=saved_symbol)
            return web.json_response(value, headers={"Cache-Control": "no-store"})
        except BudgetValidationError as exc:
            return web.json_response({"ok": False, "error": str(exc)}, status=400)
        except (ValueError, TypeError, KeyError, RuntimeError):
            return web.json_response({"ok": False, "error": "预算配置无法解析或校验，请检查配置文件后重试"}, status=400)
        except OSError:
            return web.json_response({"ok": False, "error": "预算文件读写失败，原配置已保留；请检查文件权限后重试"}, status=500)


    async def local_origin_only(self, request: web.Request, handler):
        # A remote page must not acquire the control token through DNS rebinding.
        host = request.host.rsplit(":", 1)[0] if ":" in request.host else request.host
        if host.lower() not in {"localhost", "127.0.0.1"}:
            raise web.HTTPForbidden(text="仅接受本机地址")
        origin = request.headers.get("Origin")
        if origin and origin not in {f"http://{request.host}", f"https://{request.host}"}:
            raise web.HTTPForbidden(text="禁止跨站控制请求")
        return await handler(request)


    async def api_start(self, request: web.Request) -> web.Response:
        self._require_control_token(request)
        payload = await self._read_object(request)
        strategy = str(payload.get("strategy") or "")
        value = await request.app["controller"].start(strategy)
        return web.json_response({"ok": True, "controller": value})


    async def api_recovery_hold(self, request: web.Request) -> web.Response:
        """Start live monitors now while deferring all quotes to a safe window."""

        self._require_control_token(request)
        payload = await self._read_object(request)
        strategy = str(payload.get("strategy") or "")
        value = await request.app["controller"].start(
            strategy,
            recovery_hold_until_session_pause=True,
        )
        return web.json_response({"ok": True, "controller": value})


    async def api_market_start(self, request: web.Request) -> web.Response:
        self._require_control_token(request)
        payload = await self._read_object(request)
        value = await request.app["controller"].start_market(
            str(payload.get("wallet_id") or "primary"),
            str(payload.get("symbol") or ""),
            str(payload.get("strategy") or ""),
        )
        return web.json_response({"ok": True, "controller": value})


    async def api_market_recovery_hold(self, request: web.Request) -> web.Response:
        """Start one reconciled market in persisted monitor-only isolation."""

        self._require_control_token(request)
        payload = await self._read_object(request)
        value = await request.app["controller"].start_market(
            str(payload.get("wallet_id") or "primary"),
            str(payload.get("symbol") or ""),
            str(payload.get("strategy") or ""),
            recovery_hold_until_session_pause=True,
        )
        return web.json_response({"ok": True, "controller": value})


    async def api_market_stop(self, request: web.Request) -> web.Response:
        self._require_control_token(request)
        payload = await self._read_object(request)
        value = await request.app["controller"].stop_market(
            str(payload.get("wallet_id") or "primary"),
            str(payload.get("symbol") or ""),
        )
        return web.json_response({"ok": True, "controller": value})


    async def api_grid_settings(self, request: web.Request) -> web.Response:
        self._require_control_token(request)
        payload = await self._read_object(request)
        portfolio = request.app["controller"]
        async with portfolio._lock:
            bot = portfolio._market_controller(
                str(payload.get("wallet_id") or "primary"),
                str(payload.get("symbol") or ""),
            )
            settings = await bot.save_grid_settings(payload.get("grid"))
        return web.json_response({"ok": True, "grid_settings": settings})


    async def api_stop(self, request: web.Request) -> web.Response:
        self._require_control_token(request)
        value = await request.app["controller"].stop()
        return web.json_response({"ok": True, "controller": value})
