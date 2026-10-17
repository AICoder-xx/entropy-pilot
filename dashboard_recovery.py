"""Controller recovery policy, durable evidence and bounded reconciliation.

The engine owns failure classification; this module consumes exact-run
records. Side effects are explicit injected dependencies, allowing offline
fault testing without launching a process or contacting the exchange.
"""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from aiohttp import web

from entropy_bot.recovery import (
    FailureCode, RecoveryState, classify_exception, failure_record_path,
    recovery_block_message,
)


def auto_reconciliation_blocker(plan: dict, *, before_apply: bool) -> tuple[str, str] | None:
    """Validate the official reconciliation plan without performing I/O."""
    if plan.get("external_activity_detected"):
        return "blocked_external_activity", "安全恢复已阻止：检测到同市场外部成交活动"
    if before_apply and not plan.get("apply_eligible"):
        count = int(plan.get("recoverable_count") or 0)
        return (str(plan.get("blocked_reason") or "blocked"),
                f"安全恢复已阻止：缺失成交 {count} 笔未通过补录门槛")
    if any(plan.get(key) for key in (
        "local_extra_count", "official_unknown_fill_count",
        "official_unknown_order_count", "official_open_orders",
        "ioc_pending_count", "order_pending_count",
    )):
        return "blocked_unresolved_orders", "安全恢复已阻止：订单身份、挂单或成交尚未完成对账"
    if not before_apply and (not plan.get("reconciliation_ok") or plan.get("recoverable_count")):
        return "blocked_reconciliation", "安全恢复已阻止：官方成交与本地账本仍未一致"
    return None


@dataclass(frozen=True)
class RecoveryDependencies:
    read_json: Callable
    atomic_write_json: Callable
    _pid_exists: Callable
    reconcile_stopped_market: Callable
    ROOT: Path
    AUTO_RECOVERY_DELAY_MS: int
    AUTO_RECOVERY_WINDOW_MS: int
    AUTO_RECOVERY_MAX_ATTEMPTS: int
    SAFE_HARD_RECOVERY_MAX_FILLS: int
    SAFE_HARD_RECOVERY_RETRY_MS: int
    _number: Callable
    _safe_reconciliation_error_label: Callable


class RecoveryControllerMixin:
    """Lifecycle adapter for one market; it never submits exchange orders."""

    def _recovery_dependencies(self) -> RecoveryDependencies:
        raise NotImplementedError

    def _load_recovery_attempts(self, now_ms: int | None = None) -> deque[int]:
        """Merge the persisted rolling window with this process' memory.

        The dashboard itself may be restarted by Windows or an external
        supervisor.  Keeping the retry window only in memory would reset the
        circuit breaker and could turn a persistent fault into a restart loop.
        """
        deps = self._recovery_dependencies()

        now_ms = now_ms or int(time.time() * 1000)
        value = deps.read_json(self.control_file, {})
        persisted = value.get("auto_recovery_attempts_ms", []) if isinstance(value, dict) else []
        candidates: set[int] = set(self._recovery_attempts_ms)
        if isinstance(persisted, list):
            for item in persisted:
                try:
                    attempt_ms = int(item)
                except (TypeError, ValueError, OverflowError):
                    continue
                if attempt_ms > 0:
                    candidates.add(attempt_ms)
        self._recovery_attempts_ms = deque(
            sorted(
                attempt_ms
                for attempt_ms in candidates
                if now_ms - attempt_ms <= deps.AUTO_RECOVERY_WINDOW_MS
            )
        )
        return self._recovery_attempts_ms


    def _put_recovery_attempts(self, value: dict, now_ms: int | None = None) -> None:
        attempts = self._load_recovery_attempts(now_ms)
        value["auto_recovery_attempts_ms"] = list(attempts)
        value["auto_recovery_count"] = len(attempts)


    def _current_summary(self, strategy: str | None) -> dict:
        deps = self._recovery_dependencies()
        if strategy not in {"market_maker", "grid"}:
            return {}
        runtime = deps.read_json(self.runtime_config, {})
        runtime_section = runtime.get("runtime") if isinstance(runtime, dict) else {}
        state_dir_value = (
            runtime_section.get("state_dir")
            if isinstance(runtime_section, dict)
            else None
        ) or "state"
        state_dir = Path(state_dir_value)
        if not state_dir.is_absolute():
            state_dir = deps.ROOT / state_dir
        path = state_dir / f"live_{strategy}_io_{self.symbol}_summary.json"
        value = deps.read_json(path, {})
        return value if isinstance(value, dict) else {}


    def _summary_for_control(self, control: dict) -> dict:
        """Return only a summary proven to belong to this controller run."""

        summary = self._current_summary(control.get("strategy"))
        if not summary:
            return {}
        expected_session = str(control.get("summary_session_id") or "")
        actual_session = str(summary.get("session_id") or "")
        if expected_session:
            if actual_session != expected_session:
                return {}
        else:
            # Legacy/current-startup fallback before the engine has published
            # its exact session binding. A prior run's summary is never enough
            # to prove a clean stop.
            started_at_ms = int(control.get("started_at_ms") or 0)
            summary_started_at_ms = int(summary.get("session_started_at_ms") or 0)
            if started_at_ms <= 0 or summary_started_at_ms < started_at_ms:
                return {}
        started_at_ms = int(control.get("started_at_ms") or 0)
        updated_at_ms = int(summary.get("updated_at_ms") or 0)
        if started_at_ms > 0 and updated_at_ms < started_at_ms:
            return {}
        return summary


    def _startup_failed_before_signed_mutation(self, control: dict) -> bool:
        """Recognize a dead child that failed in read-only startup checks."""
        deps = self._recovery_dependencies()

        if deps._pid_exists(int(control.get("pid") or 0)):
            return False
        run_id = str(control.get("run_id") or "")
        record = deps.read_json(failure_record_path(self.control_file, run_id), {})
        state = RecoveryState.from_record(record.get("recovery"), run_id=run_id) if isinstance(record, dict) else None
        return state is not None and state.startup_read_only


    async def _gate_previous_session_summary(self) -> None:
        """Converge a prior session from the user's normal Start action.

        A dead child can leave its last summary as ``running`` or ``paused``.
        The green Start button is authorization to run the bounded, idempotent
        ledger repair already used by unattended recovery.  Unknown ownership,
        external activity, open orders, or an over-limit repair still fail
        closed, but routine recovery never sends the user to a Python command.
        """
        deps = self._recovery_dependencies()

        control = deps.read_json(self.control_file, {})
        if not isinstance(control, dict):
            return
        raw_summary = self._current_summary(control.get("strategy"))
        if not raw_summary:
            return
        if control.get("strategy") == "grid":
            # The stopped-fill repair tool addresses the market-maker ledger.
            # Grid restarts reconcile their durable inventory/fill journal in
            # the engine before quoting; never rewrite them through that tool.
            if raw_summary.get("open_order_details") or deps._number(raw_summary.get("open_orders")):
                raise web.HTTPConflict(text="上一网格会话仍有未完成委托，请先完成撤单对账；库存台账已保留")
            if raw_summary.get("grid_accounting", {}).get("status") == "error":
                raise web.HTTPConflict(text="上一网格库存台账需要对账，已阻止启动并保留真实库存")
            return
        # This is the explicit Start action's reconciliation gate. An old
        # summary is never evidence of a safe exit, but it must not trap the
        # operator after replacing an interrupted signing wallet. Prove the
        # current official order/fill state below regardless of summary age.
        # Automatic callers separately require an exact-run typed terminal
        # record before they may reach Start at all.
        try:
            plan = await deps.reconcile_stopped_market(
                self.symbol,
                wallet_id=self.wallet_id,
                apply=False,
                max_fills=deps.SAFE_HARD_RECOVERY_MAX_FILLS,
            )
        except Exception as exc:
            raise web.HTTPConflict(
                text=(
                    "启动前停机对账不可用"
                    f"（{deps._safe_reconciliation_error_label(exc)}）。"
                    "请检查面板中的钱包配置；旧会话已保留，面板未发送订单"
                )
            ) from exc
        pre_apply_blockers = {
            "local_extra_count": int(plan.get("local_extra_count") or 0),
            "official_unknown_fill_count": int(
                plan.get("official_unknown_fill_count") or 0
            ),
            "official_unknown_order_count": int(
                plan.get("official_unknown_order_count") or 0
            ),
            "official_open_orders": int(plan.get("official_open_orders") or 0),
            "ioc_pending_count": int(plan.get("ioc_pending_count") or 0),
            "order_pending_count": int(plan.get("order_pending_count") or 0),
        }
        recoverable_count = int(plan.get("recoverable_count") or 0)
        if (
            recoverable_count
            and bool(plan.get("apply_eligible"))
            and not any(pre_apply_blockers.values())
        ):
            try:
                plan = await deps.reconcile_stopped_market(
                    self.symbol,
                    wallet_id=self.wallet_id,
                    apply=True,
                    max_fills=deps.SAFE_HARD_RECOVERY_MAX_FILLS,
                )
            except Exception as exc:
                raise web.HTTPConflict(
                    text=(
                        "一键启动自动补齐停机成交失败"
                        f"（{deps._safe_reconciliation_error_label(exc)}）；"
                        "旧会话已保留，面板未发送订单"
                    )
                ) from exc
        blockers = {
            "recoverable_count": int(plan.get("recoverable_count") or 0),
            "local_extra_count": int(plan.get("local_extra_count") or 0),
            "official_unknown_fill_count": int(
                plan.get("official_unknown_fill_count") or 0
            ),
            "official_unknown_order_count": int(
                plan.get("official_unknown_order_count") or 0
            ),
            "official_open_orders": int(plan.get("official_open_orders") or 0),
            "ioc_pending_count": int(plan.get("ioc_pending_count") or 0),
            "order_pending_count": int(plan.get("order_pending_count") or 0),
        }
        blocked = (
            not bool(plan.get("reconciliation_ok"))
            or any(blockers.values())
        )
        if blocked:
            detail = ", ".join(
                f"{key}={value}" for key, value in blockers.items() if value
            ) or str(plan.get("blocked_reason") or "reconciliation_not_clean")
            raise web.HTTPConflict(
                text=(
                    f"启动前停机对账未收敛（{detail}）；"
                    "旧会话已保留，面板未发送订单"
                )
            )


    def _converge_requested_stop(
        self, run_id: str, pid: int, *, now_ms: int | None = None
    ) -> None:
        """Converge an exited stop request from the current run's final summary."""
        deps = self._recovery_dependencies()

        value = deps.read_json(self.control_file, {})
        if not (
            isinstance(value, dict)
            and run_id
            and value.get("run_id") == run_id
            and value.get("desired_state") == "stop_requested"
        ):
            return
        now_ms = now_ms or int(time.time() * 1000)
        summary = self._summary_for_control(value)
        summary_status = summary.get("status")
        open_orders = summary.get("open_orders")
        clean = summary_status == "stopped" and open_orders == 0
        if clean:
            value["desired_state"] = "stopped"
            value["process_state"] = "stopped"
            value["message"] = "已安全停止（官方挂单终态已确认）"
        else:
            reason = str(summary.get("reason") or "")
            if summary_status == "halted" and reason:
                message = reason
            elif summary_status == "stopped":
                message = "退出摘要未确认挂单已清空，需要核对官方挂单"
            else:
                message = "机器人已退出，但当前运行的最终摘要缺失或未确认干净"
            value["desired_state"] = "halted"
            value["process_state"] = "halted"
            value["message"] = f"安全停机未收敛：{message}"
        value["dashboard_heartbeat_ms"] = now_ms
        value.pop("unexpected_exit", None)
        value.pop("unexpected_exit_at_ms", None)
        if self.process is not None and self.process.pid == pid:
            exit_code = self.process.poll()
            if exit_code is not None:
                value["last_exit_code"] = exit_code
        deps.atomic_write_json(self.control_file, value)


    def _recovery_state_for_control(self, control: dict) -> RecoveryState | None:
        deps = self._recovery_dependencies()
        run_id = str(control.get("run_id") or "")
        summary = self._summary_for_control(control)
        # The most recent terminal engine summary owns the order-clean proof.
        # A nonterminal snapshot before a process crash cannot grant recovery.
        if summary.get("status") in {"halted", "stopped"}:
            return RecoveryState.from_record(summary.get("recovery"), run_id=run_id)
        record = deps.read_json(failure_record_path(self.control_file, run_id), {})
        return RecoveryState.from_record(record.get("recovery"), run_id=run_id) if isinstance(record, dict) else None


    def _block_auto_recovery(self, reason: str) -> None:
        deps = self._recovery_dependencies()
        value = deps.read_json(self.control_file, {})
        if not isinstance(value, dict):
            return
        value["desired_state"] = "halted"
        value["process_state"] = "halted"
        value["message"] = f"自动恢复已阻止：{reason}"
        value["dashboard_heartbeat_ms"] = int(time.time() * 1000)
        deps.atomic_write_json(self.control_file, value)

    def _automatic_attempt_is_current(self, original: dict) -> bool:
        deps = self._recovery_dependencies()
        latest = deps.read_json(self.control_file, {})
        return (
            isinstance(latest, dict)
            and latest.get("run_id") == original.get("run_id")
            and latest.get("stop_generation", 0) == original.get("stop_generation", 0)
            and latest.get("auto_recovery_disabled") is not True
            and latest.get("desired_state") == original.get("desired_state")
        )


    async def auto_recover_once(self) -> bool:
        """Restart one crashed child only after proving it was not a risk halt."""
        deps = self._recovery_dependencies()

        if self._recovering:
            return False
        status = self.status()
        if (
            status.get("running")
            or status.get("auto_recovery_disabled")
            or status.get("desired_state") != "running"
            or status.get("process_state") != "exited"
        ):
            return False
        value = deps.read_json(self.control_file, {})
        if not isinstance(value, dict):
            return False
        now_ms = int(time.time() * 1000)
        exited_at_ms = int(value.get("unexpected_exit_at_ms") or now_ms)
        if now_ms - exited_at_ms < deps.AUTO_RECOVERY_DELAY_MS:
            return False
        if value.get("run_id") != status.get("run_id") or not self._automatic_attempt_is_current(value):
            return False
        state = self._recovery_state_for_control(value)
        if state is None or not state.automatic_recovery_allowed:
            self._block_auto_recovery(recovery_block_message(state) or "当前故障不允许自动恢复")
            return False
        attempts = self._load_recovery_attempts(now_ms)
        if len(attempts) >= deps.AUTO_RECOVERY_MAX_ATTEMPTS:
            self._block_auto_recovery("一小时内瞬时恢复已达到3次上限")
            return False
        strategy = status.get("strategy")
        if strategy not in {"market_maker", "grid"}:
            self._block_auto_recovery("缺少可恢复的策略类型")
            return False
        attempts.append(now_ms)
        value["auto_recovery_attempts_ms"] = list(attempts)
        value["auto_recovery_count"] = len(attempts)
        deps.atomic_write_json(self.control_file, value)
        self._recovering = True
        try:
            recovery_hold = (
                value.get("recovery_hold_until_session_pause") is True
            )
            if recovery_hold:
                await self.start(
                    strategy,
                    recovery_hold_until_session_pause=True,
                    _expected_recovery=value,
                )
            else:
                await self.start(strategy, _expected_recovery=value)
            return True
        except Exception as exc:
            failed = deps.read_json(self.control_file, {})
            if isinstance(failed, dict) and self._automatic_attempt_is_current(value):
                # A failed start may have crossed the signing boundary. Never
                # reclassify it using the previous run's transport evidence.
                failed["desired_state"] = "halted"
                failed["process_state"] = "halted"
                failed["message"] = (
                    f"自动恢复启动失败：{type(exc).__name__}，需重新核验后手动启动"
                )
                deps.atomic_write_json(self.control_file, failed)
            return False
        finally:
            self._recovering = False


    def _record_safe_hard_recovery_outcome(
        self, run_id: str, outcome: str, message: str
    ) -> None:
        deps = self._recovery_dependencies()
        value = deps.read_json(self.control_file, {})
        if not isinstance(value, dict) or value.get("run_id") != run_id:
            return
        value["safe_hard_recovery_checked_run_id"] = run_id
        value["safe_hard_recovery_outcome"] = outcome
        value["message"] = message
        value["dashboard_heartbeat_ms"] = int(time.time() * 1000)
        deps.atomic_write_json(self.control_file, value)


    async def auto_recover_safe_halt_once(self) -> bool:
        """Recover a typed, reconciled network halt through the official gate."""
        deps = self._recovery_dependencies()

        if self._hard_recovering:
            return False
        status = self.status()
        if (
            status.get("running")
            or status.get("auto_recovery_disabled")
            or status.get("desired_state") != "halted"
            or status.get("process_state") != "halted"
            or status.get("strategy") != "market_maker"
        ):
            return False
        value = deps.read_json(self.control_file, {})
        if not isinstance(value, dict):
            return False
        run_id = str(value.get("run_id") or "")
        if not run_id or value.get("safe_hard_recovery_checked_run_id") == run_id:
            return False
        now_ms = int(time.time() * 1000)
        if now_ms < int(value.get("safe_hard_recovery_retry_after_ms") or 0):
            return False
        summary = self._summary_for_control(value)
        state = self._recovery_state_for_control(value)
        if summary.get("status") != "halted" or state is None or not state.automatic_recovery_allowed:
            return False
        updated_at_ms = int(summary.get("updated_at_ms") or 0)
        if updated_at_ms and now_ms - updated_at_ms < deps.AUTO_RECOVERY_DELAY_MS:
            return False
        # Shared rolling-hour circuit breaker with the unexpected-exit
        # recovery: a persistent fault must escalate to a human instead of
        # restart-looping against the exchange.
        attempts = self._load_recovery_attempts(now_ms)
        if len(attempts) >= deps.AUTO_RECOVERY_MAX_ATTEMPTS:
            self._record_safe_hard_recovery_outcome(
                run_id,
                "blocked_attempt_limit",
                "安全恢复已阻止：一小时内自动恢复已达 3 次上限，需人工介入",
            )
            return False

        self._hard_recovering = True
        try:
            plan = await deps.reconcile_stopped_market(
                self.symbol,
                wallet_id=self.wallet_id,
                apply=False,
                max_fills=deps.SAFE_HARD_RECOVERY_MAX_FILLS,
            )
            if not self._automatic_attempt_is_current(value):
                return False
            blocker = auto_reconciliation_blocker(plan, before_apply=True)
            if blocker:
                self._record_safe_hard_recovery_outcome(run_id, *blocker)
                return False
            if int(plan.get("recoverable_count") or 0):
                plan = await deps.reconcile_stopped_market(
                    self.symbol,
                    wallet_id=self.wallet_id,
                    apply=True,
                    max_fills=deps.SAFE_HARD_RECOVERY_MAX_FILLS,
                )
                if not self._automatic_attempt_is_current(value):
                    return False
            blocker = auto_reconciliation_blocker(plan, before_apply=False)
            if blocker:
                self._record_safe_hard_recovery_outcome(run_id, *blocker)
                return False
            # A non-zero official position deliberately does NOT block
            # recovery: the engine's startup reconciliation adopts the
            # clearinghouse position as the authority, and the loss lines
            # plus flatten_on_halt bound any residual risk.  Leaving
            # inventory unmanaged while halted is the greater danger.
            attempts.append(now_ms)
            value["auto_recovery_attempts_ms"] = list(attempts)
            value["auto_recovery_count"] = len(attempts)
            deps.atomic_write_json(self.control_file, value)
            # A 7x24 market (no US-session pause) would never leave the
            # monitor-only recovery hold, so it restarts normally; gated
            # markets keep the conservative hold-until-session-pause.
            hold = bool(
                (self.runtime_overrides.get("runtime") or {}).get(
                    "pause_during_us_regular_session", True
                )
            )
            await self.start(
                "market_maker",
                recovery_hold_until_session_pause=hold,
                _expected_recovery=value,
            )
            return True
        except Exception as exc:
            latest = deps.read_json(self.control_file, {})
            if isinstance(latest, dict) and self._automatic_attempt_is_current(value):
                if classify_exception(exc) is FailureCode.TRANSPORT_UNAVAILABLE:
                    latest["safe_hard_recovery_retry_after_ms"] = (
                        int(time.time() * 1000) + deps.SAFE_HARD_RECOVERY_RETRY_MS
                    )
                else:
                    latest["safe_hard_recovery_checked_run_id"] = run_id
                    latest["safe_hard_recovery_outcome"] = "blocked_unclassified_error"
                latest["message"] = (
                    f"安全恢复核验未完成：{type(exc).__name__}；仅网络错误允许退避重查"
                )
                deps.atomic_write_json(self.control_file, latest)
            return False
        finally:
            self._hard_recovering = False
