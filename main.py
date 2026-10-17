from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path

from entropy_bot.config import ConfigError, load_config
from entropy_bot.engine import Engine, check_config_live_data
from entropy_bot.storage import atomic_write_json, read_json, setup_logging
from entropy_bot.recovery import FailureCode, RecoveryState, failure_record_path


def _utf8_console() -> None:
    for name in ("stdout", "stderr"):
        stream = getattr(sys, name, None)
        if stream is not None and getattr(stream, "encoding", "").lower() != "utf-8":
            try:
                stream.reconfigure(encoding="utf-8")
            except Exception:
                pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Entropy io 自动做市 / 网格机器人")
    parser.add_argument(
        "--config",
        default="config.json",
        help="JSON 配置文件（默认 config.json）",
    )
    parser.add_argument("--check", action="store_true", help="只读核验市场、费率和配置，不下单")
    parser.add_argument("--duration", type=float, help="运行指定秒数后正常退出（模拟测试用）")
    parser.add_argument("--verbose", action="store_true", help="详细日志")
    parser.add_argument("--control-file", help=argparse.SUPPRESS)
    parser.add_argument("--run-id", help=argparse.SUPPRESS)
    return parser.parse_args()


def register_control_process(args: argparse.Namespace) -> bool:
    """Claim a prepared control intent before config/network startup.

    Recovery workers prepare ``run_id`` and ``desired_state`` first, then
    spawn this child.  Registering our own PID immediately closes the small
    interval in which the dashboard could otherwise mistake PID=None for a
    crashed recovery attempt.
    """
    if bool(args.control_file) != bool(args.run_id):
        print("控制参数错误: --control-file 与 --run-id 必须同时提供", file=sys.stderr)
        return False
    if not args.control_file:
        return True

    control_path = Path(args.control_file).resolve()
    try:
        value = read_json(control_path, None, strict=True)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"控制文件错误: {type(exc).__name__}: {exc}", file=sys.stderr)
        return False
    if not isinstance(value, dict):
        print("控制文件错误: 内容不是对象", file=sys.stderr)
        return False
    if value.get("run_id") != args.run_id or value.get("desired_state") != "running":
        print("控制状态已变化，拒绝启动过期进程", file=sys.stderr)
        return False

    value["pid"] = os.getpid()
    value["process_state"] = "starting"
    value["dashboard_heartbeat_ms"] = int(time.time() * 1000)
    value["message"] = "机器人进程已登记，正在执行启动核验"
    value.pop("unexpected_exit", None)
    atomic_write_json(control_path, value)
    return True


async def async_main(args: argparse.Namespace) -> int:
    try:
        cfg = load_config(Path(args.config))
    except (OSError, json.JSONDecodeError, ConfigError) as exc:
        if args.control_file and args.run_id:
            atomic_write_json(
                failure_record_path(Path(args.control_file).resolve(), args.run_id),
                {
                    "recovery": RecoveryState(
                        FailureCode.CONFIGURATION_INVALID, startup_read_only=True
                    ).as_dict(args.run_id),
                    "updated_at_ms": int(time.time() * 1000),
                },
            )
        print(f"配置错误: {exc}", file=sys.stderr)
        return 2
    setup_logging(cfg.runtime.log_dir, args.verbose)
    if args.check:
        result = await check_config_live_data(cfg)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    engine = Engine(
        cfg,
        control_file=Path(args.control_file).resolve() if args.control_file else None,
        run_id=args.run_id,
    )
    await engine.run(args.duration)
    return 3 if engine.status == "halted" else 0


def main() -> int:
    _utf8_console()
    if sys.platform == "win32" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
        # Selector avoids noisy Proactor connection-reset callbacks when the
        # remote websocket closes during a normal Ctrl+C / timed paper exit.
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    args = parse_args()
    if not register_control_process(args):
        return 2
    try:
        return asyncio.run(async_main(args))
    except KeyboardInterrupt:
        print("\n收到 Ctrl+C，正在撤销本机器人挂单并退出……")
        return 130
    except Exception as exc:
        # Never print environment values or raw request bodies here.
        print(f"启动失败: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
