"""Require explicit branch coverage for failure handling and persisted order intent."""
from __future__ import annotations

import json
from pathlib import Path
import sys

# A per-module branch floor prevents UI coverage from hiding regressions here.
# Platform-specific paths are measured on each OS; absence is not reported as zero-risk.
BRANCH_FLOORS = {
    "entropy_bot/storage.py": 75.0,
    "entropy_bot/recovery.py": 85.0,
    "entropy_bot/signing_lock.py": 70.0,
}


def main(path: str) -> int:
    report = json.loads(Path(path).read_text(encoding="utf-8"))
    files = {key.replace("\\", "/"): value for key, value in report["files"].items()}
    failures = []
    for filename, minimum in BRANCH_FLOORS.items():
        record = files.get(filename)
        if record is None:
            failures.append(filename + ": missing from coverage measurement")
            continue
        summary = record["summary"]
        total = summary["num_branches"]
        covered = summary["covered_branches"]
        percent = 100.0 * covered / total if total else 100.0
        print(f"{filename}: branches {covered}/{total} = {percent:.2f}% (minimum {minimum:.0f}%)")
        if percent < minimum:
            failures.append(filename + ": branch floor not met")
    for message in failures:
        print(message, file=sys.stderr)
    return bool(failures)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1]))
