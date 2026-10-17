"""One writer per real account/market, shared by GUI and CLI strategies."""
from __future__ import annotations

import hashlib
import os
from pathlib import Path


class InstanceLock:
    def __init__(self, directory: Path, identity: str):
        directory.mkdir(parents=True, exist_ok=True)
        self.path = directory / (hashlib.sha256(identity.encode()).hexdigest() + ".lock")
        self.handle = None

    def acquire(self) -> None:
        handle = self.path.open("a+b")
        if handle.seek(0, 2) == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            handle.close()
            raise RuntimeError("同一账户和市场已有策略运行；请先停止原策略，不能并行运行做市与网格") from exc
        self.handle = handle

    def close(self) -> None:
        if self.handle is not None:
            self.handle.close()  # OS releases the lock even after a crash.
            self.handle = None
