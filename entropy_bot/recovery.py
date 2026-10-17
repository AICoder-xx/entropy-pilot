"""Stable, fail-closed recovery policy shared by engine and dashboard.

Display messages are deliberately absent from every decision. Persisted
records must match this schema and the exact run before they authorize even
a reconciliation attempt; order submission still requires normal startup.
"""
from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Mapping

import aiohttp


class FailureCode(str, Enum):
    NONE = "NONE"
    TRANSPORT_UNAVAILABLE = "TRANSPORT_UNAVAILABLE"
    RISK_LIMIT = "RISK_LIMIT"
    AUTHORIZATION_FAILED = "AUTHORIZATION_FAILED"
    CONFIGURATION_INVALID = "CONFIGURATION_INVALID"
    ORDER_STATE_UNCERTAIN = "ORDER_STATE_UNCERTAIN"
    PROTOCOL_FAILURE = "PROTOCOL_FAILURE"
    UNEXPECTED_EXCEPTION = "UNEXPECTED_EXCEPTION"
    RECOVERY_HOLD_VIOLATION = "RECOVERY_HOLD_VIOLATION"
    PROCESS_EXIT_UNVERIFIED = "PROCESS_EXIT_UNVERIFIED"
    SIGNING_LOCK_UNAVAILABLE = "SIGNING_LOCK_UNAVAILABLE"
    SIGNING_LOCK_TIMEOUT = "SIGNING_LOCK_TIMEOUT"
    SIGNING_IDENTITY_UNAVAILABLE = "SIGNING_IDENTITY_UNAVAILABLE"
    SIGNING_INTERRUPTED = "SIGNING_INTERRUPTED"
    PAID_ACTION_RECOVERY_UNAVAILABLE = "PAID_ACTION_RECOVERY_UNAVAILABLE"


def failure_record_path(control_file: Path, run_id: str) -> Path:
    """Separate child-owned evidence avoids races with dashboard heartbeats."""
    fingerprint = hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:24]
    return control_file.with_name(f"{control_file.stem}.{fingerprint}.failure.json")


def classify_exception(exc: BaseException) -> FailureCode:
    """Recognize direct transport types, never diagnostic text or causes.

    A business/protocol exception raised *from* a timeout is still a business
    failure. TLS/certificate failures also require intervention. Broker codes
    may become more specific over time; an unfamiliar code always stays closed.
    """
    if isinstance(exc, (aiohttp.ClientSSLError, aiohttp.ServerFingerprintMismatch)):
        return FailureCode.PROTOCOL_FAILURE
    if isinstance(exc, aiohttp.ClientResponseError):
        if exc.status in {401, 403}:
            return FailureCode.AUTHORIZATION_FAILED
        if exc.status in {408, 425, 429} or 500 <= exc.status <= 599:
            return FailureCode.TRANSPORT_UNAVAILABLE
        return FailureCode.PROTOCOL_FAILURE
    code = getattr(exc, "code", None)
    if code is not None:
        try:
            return FailureCode(code)
        except (ValueError, TypeError):
            return FailureCode.UNEXPECTED_EXCEPTION
    if isinstance(exc, (aiohttp.ClientConnectionError, asyncio.TimeoutError)):
        return FailureCode.TRANSPORT_UNAVAILABLE
    return FailureCode.UNEXPECTED_EXCEPTION


@dataclass(frozen=True)
class RecoveryState:
    code: FailureCode = FailureCode.NONE
    orders_reconciled: bool = False
    startup_read_only: bool = False

    def as_dict(self, run_id: str) -> dict:
        return {
            "schema_version": 1,
            "run_id": run_id,
            "code": self.code.value,
            "orders_reconciled": self.orders_reconciled,
            "startup_read_only": self.startup_read_only,
        }

    @classmethod
    def from_record(cls, value: object, *, run_id: str) -> RecoveryState | None:
        if not isinstance(value, Mapping) or not run_id:
            return None
        if type(value.get("schema_version")) is not int or value["schema_version"] != 1:
            return None
        if value.get("run_id") != run_id:
            return None
        if type(value.get("orders_reconciled")) is not bool:
            return None
        if type(value.get("startup_read_only")) is not bool:
            return None
        try:
            code = FailureCode(value.get("code"))
        except (ValueError, TypeError):
            return None
        return cls(code, value["orders_reconciled"], value["startup_read_only"])

    @property
    def automatic_recovery_allowed(self) -> bool:
        return self.code is FailureCode.TRANSPORT_UNAVAILABLE and self.orders_reconciled


def recovery_block_message(state: RecoveryState | None) -> str | None:
    """Presentation for a policy outcome; callers never parse this message."""
    if state is None:
        return "缺少当前运行的有效恢复分类；请先在面板执行停机对账后手动启动"
    if state.code is FailureCode.SIGNING_INTERRUPTED:
        return (
            "签名进程曾异常中断；请先在交易所官网核对订单和系统时间，"
            "再在钱包配置中更换新的专用 API 钱包。保留原资金账户和账本，"
            "未确认订单继续对账；程序不会自动清除签名保护"
        )
    if state.code is not FailureCode.TRANSPORT_UNAVAILABLE:
        return f"故障代码 {state.code.value} 不允许自动恢复；请核对停机状态"
    if not state.orders_reconciled:
        return "订单终态尚未确认；保留订单身份，需先完成停机对账"
    return None
