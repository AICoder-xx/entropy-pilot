from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from pathlib import Path

import aiohttp

import audit_live as audit
from entropy_bot.broker import PendingOrderReconciliation, order_ownership
from entropy_bot.config import load_config
from entropy_bot.credentials import resolve_live_credentials
from entropy_bot.engine import Engine
from entropy_bot.market import MarketClient


ROOT = Path(__file__).resolve().parent

if sys.stdout and sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass


def paths_for(symbol: str) -> tuple[Path, Path]:
    slug = symbol.lower()
    return (
        ROOT / f"config.live.ui.{slug}.runtime.json",
        ROOT / "state" / f"ui_control_{slug}.json",
    )


def build_recovery_plan(
    *,
    process_running: bool,
    ownerships: list[str],
    max_orders: int,
) -> dict:
    counts = {
        "bot": ownerships.count("bot"),
        "external": ownerships.count("external"),
        "unknown": ownerships.count("unknown"),
    }
    blocked_reasons: list[str] = []
    if process_running:
        blocked_reasons.append("process_still_running")
    if counts["external"]:
        blocked_reasons.append("external_open_order_present")
    if counts["unknown"]:
        blocked_reasons.append("unknown_open_order_ownership")
    if counts["bot"] > max(max_orders, 0):
        blocked_reasons.append("bot_open_order_limit_exceeded")
    return {
        "official_open_orders": len(ownerships),
        "official_bot_open_orders": counts["bot"],
        "official_external_open_orders": counts["external"],
        "official_unknown_open_orders": counts["unknown"],
        "apply_eligible": not blocked_reasons,
        "blocked_reason": blocked_reasons[0] if blocked_reasons else None,
        "blocked_reasons": blocked_reasons,
    }


async def classify_open_orders(
    market: MarketClient,
    address: str,
    accepted: set[str],
) -> tuple[list[dict], list[str]]:
    rows = [
        row
        for row in await market.open_orders(address)
        if row.get("coin") in accepted
    ]
    ownerships: list[str] = []
    for row in rows:
        try:
            oid = int(row.get("oid") or 0)
            status = await market.order_status(address, oid)
        except Exception:
            status = None
        ownership, _ = order_ownership(status)
        ownerships.append(ownership)
    return rows, ownerships


async def _cancel_once_and_confirm(cfg, *, timeout_sec: float) -> None:
    """Use one broker instance so a pending cancel is never resubmitted."""
    engine = Engine(cfg)
    pending = False
    try:
        try:
            await engine.initialize()
        except PendingOrderReconciliation:
            pending = True
        if not engine.broker or not engine.market:
            raise RuntimeError("取消器初始化未完成")
        if pending:
            deadline = time.monotonic() + max(timeout_sec, 1.0)
            while True:
                if time.monotonic() >= deadline:
                    raise RuntimeError("机器人遗留挂单在安全窗口内仍未确认撤销")
                await asyncio.sleep(1.0)
                try:
                    await engine.broker.cancel_bot_orphans()  # type: ignore[attr-defined]
                    break
                except PendingOrderReconciliation:
                    continue
        else:
            # initialize() normally performs this cleanup. Calling it once
            # more is a read-only no-op after the authoritative view is empty.
            await engine.broker.cancel_bot_orphans()  # type: ignore[attr-defined]
    finally:
        if engine.session and not engine.session.closed:
            await engine.session.close()


async def recover(
    symbol: str,
    *,
    apply: bool,
    max_orders: int = 10,
    timeout_sec: float = 65.0,
) -> dict:
    config_path, control_path = paths_for(symbol)
    control = json.loads(control_path.read_text(encoding="utf-8"))
    process_running = audit.process_alive(int(control.get("pid") or 0))
    cfg = load_config(config_path)
    credentials = resolve_live_credentials(cfg, require_private_key=apply)
    accepted = {cfg.market.symbol, f"{cfg.market.dex}:{cfg.market.symbol}"}

    async with aiohttp.ClientSession(trust_env=False) as session:
        market = MarketClient(cfg.market, session, info_capacity=180)
        _, ownerships = await classify_open_orders(
            market,
            credentials.account_address,
            accepted,
        )
    plan = build_recovery_plan(
        process_running=process_running,
        ownerships=ownerships,
        max_orders=max_orders,
    )
    if apply and not plan["apply_eligible"]:
        raise RuntimeError(
            f"拒绝撤单恢复：{plan['blocked_reason'] or '安全条件未满足'}"
        )

    canceled = 0
    if apply and plan["official_bot_open_orders"]:
        logging.disable(logging.CRITICAL)
        try:
            await _cancel_once_and_confirm(cfg, timeout_sec=timeout_sec)
        finally:
            logging.disable(logging.NOTSET)
        canceled = int(plan["official_bot_open_orders"])

    # Always take a fresh official view after an apply; the result is safe to
    # print because it contains counts only, never order identifiers.
    credentials = resolve_live_credentials(cfg, require_private_key=False)
    async with aiohttp.ClientSession(trust_env=False) as session:
        market = MarketClient(cfg.market, session, info_capacity=180)
        final_rows, final_ownerships = await classify_open_orders(
            market,
            credentials.account_address,
            accepted,
        )
    result = {
        "symbol": symbol,
        "applied": apply,
        "canceled_count": canceled,
        **plan,
        "final_official_open_orders": len(final_rows),
        "final_official_bot_open_orders": final_ownerships.count("bot"),
        "final_official_external_open_orders": final_ownerships.count("external"),
        "final_official_unknown_open_orders": final_ownerships.count("unknown"),
    }
    if apply and result["final_official_open_orders"]:
        raise RuntimeError("撤单恢复后官方仍有未结订单")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="安全撤销已停机 Entropy 机器人的遗留订单"
    )
    parser.add_argument("--symbol", required=True, choices=("SNDK", "ANTH"))
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--max-orders", type=int, default=10)
    parser.add_argument("--timeout-sec", type=float, default=65.0)
    args = parser.parse_args()
    print(
        json.dumps(
            asyncio.run(
                recover(
                    args.symbol,
                    apply=args.apply,
                    max_orders=max(args.max_orders, 0),
                    timeout_sec=max(args.timeout_sec, 1.0),
                )
            ),
            ensure_ascii=False,
            separators=(",", ":"),
        )
    )


if __name__ == "__main__":
    main()
