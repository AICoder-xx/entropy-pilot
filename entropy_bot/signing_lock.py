"""OS-owned signer locks, shared across markets, accounts and app checkouts.

The exchange nonce domain is the signing wallet, including API wallets. Files
contain only a hashed identity and the last completed-call timestamp. Never
unlink a lock file: replacing its inode would create a second lock domain.
"""
from __future__ import annotations

import errno
import hashlib
import json
import math
import os
import re
import sys
import time
from pathlib import Path
from typing import BinaryIO, Callable, TypeVar

T = TypeVar("T")


class SigningLockError(RuntimeError):
    code = "SIGNING_LOCK_UNAVAILABLE"


class SigningLockTimeout(SigningLockError):
    code = "SIGNING_LOCK_TIMEOUT"


class SigningIdentityUnavailable(SigningLockError):
    code = "SIGNING_IDENTITY_UNAVAILABLE"


class SigningInterrupted(SigningLockError):
    code = "SIGNING_INTERRUPTED"

    def __init__(self) -> None:
        super().__init__(
            "上次签名进程异常中止或系统时间倒退，当前签名身份已锁定；"
            "请在交易所官网核对订单和系统时间，然后在钱包配置中更换新的专用 API 钱包，"
            "保持原资金账户和账本；不要删除签名锁文件"
        )


class SignerProcessLock:
    """Serialize the entire synchronous SDK call, including nonce generation.

    This is a same-OS-user, same-machine lock. Other trading software and remote
    machines do not participate; dedicate an API signing wallet to this app.
    POSIX flock and Windows byte-range locks are released by OS process death.
    """

    def __init__(
        self, signer_address: str, *, directory: Path | None = None,
        timeout: float = 30.0,
    ) -> None:
        if not re.fullmatch(r"0x[0-9a-fA-F]{40}", signer_address):
            raise SigningIdentityUnavailable("缺少有效签名身份；拒绝签名")
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("signing lock timeout must be positive and finite")
        root = directory if directory is not None else Path.home() / ".entropy-trader" / "signing-locks"
        digest = hashlib.sha256(signer_address.lower().encode("ascii")).hexdigest()
        self.path = root / (digest + ".lock")
        self.timeout = timeout

    @staticmethod
    def _try_acquire(handle: BinaryIO) -> None:
        if sys.platform == "win32":
            import msvcrt
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        elif os.name == "posix":
            import fcntl
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        else:
            raise SigningLockError("当前系统不支持交易签名进程锁；拒绝签名")

    @staticmethod
    def _record(handle: BinaryIO, state: str, timestamp: int) -> None:
        payload = json.dumps({"version": 1, "state": state, "time_ms": timestamp}).encode("ascii")
        handle.seek(0)
        if handle.write(payload) != len(payload):
            raise SigningLockError("签名锁持久记录未完整写入；拒绝继续签名")
        handle.truncate()
        os.fsync(handle.fileno())

    @staticmethod
    def _previous_time(handle: BinaryIO) -> int:
        handle.seek(0)
        raw = handle.read(256)
        if not raw:
            return 0
        try:
            record = json.loads(raw)
            # Read a completed timestamp from the first file-lock format.
            if type(record) is int and record >= 0:
                return record
            if (
                not isinstance(record, dict)
                or set(record) != {"version", "state", "time_ms"}
                or type(record["version"]) is not int or record["version"] != 1
                or type(record["time_ms"]) is not int or record["time_ms"] < 0
                or record["state"] not in {"in_progress", "completed"}
            ):
                raise ValueError
            if record["state"] == "in_progress":
                raise SigningInterrupted()
            return int(record["time_ms"])
        except (ValueError, TypeError, UnicodeError) as exc:
            raise SigningLockError("签名锁时间记录损坏；拒绝签名") from exc

    def run(self, fn: Callable[..., T], *args: object) -> T:
        deadline = time.monotonic() + self.timeout
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
            handle = os.fdopen(fd, "r+b", buffering=0)
        except OSError as exc:
            raise SigningLockError("无法打开交易签名进程锁；拒绝签名") from exc
        with handle:
            # Windows locks may extend beyond EOF, so no unlocked initialization
            # writes are needed. All reads and writes occur after acquisition.
            while True:
                try:
                    self._try_acquire(handle)
                    break
                except OSError as exc:
                    if exc.errno not in {errno.EACCES, errno.EAGAIN, errno.EDEADLK}:
                        raise SigningLockError("无法取得交易签名进程锁；拒绝签名") from exc
                    if time.monotonic() >= deadline:
                        raise SigningLockTimeout("等待交易签名进程锁超时；未调用签名器") from exc
                    time.sleep(min(0.01, max(0.0, deadline - time.monotonic())))
            previous_ms = self._previous_time(handle)
            # Bound rollback waits for a completed call. An interrupted call's
            # in-progress marker already failed closed above.
            after_ms = max(previous_ms, int(time.time() * 1000))
            while int(time.time() * 1000) <= after_ms:
                if time.monotonic() >= deadline:
                    raise SigningLockTimeout("系统时间未越过上次签名时间；拒绝签名")
                time.sleep(0.001)
            started_ms = int(time.time() * 1000)
            # fsync BEFORE calling the SDK: process death may skip finally and
            # a later wall-clock rollback must not reuse an unknown signed nonce.
            self._record(handle, "in_progress", started_ms)
            try:
                return fn(*args)
            finally:
                finished_ms = int(time.time() * 1000)
                if finished_ms < started_ms:
                    # Keep the durable marker; no manual file deletion/reset.
                    raise SigningInterrupted()
                # Ordinary SDK errors settle the thread and can clear this
                # signer gate. Their order intents still need reconciliation.
                self._record(handle, "completed", finished_ms)
