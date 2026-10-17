"""Run reproducible, offline engineering checks from a checkout or source package."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def run(*args: str) -> None:
    print("+ " + " ".join(args), flush=True)
    subprocess.run(args, cwd=ROOT, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="reports/quality", help="Local, ignored evidence directory")
    args = parser.parse_args()
    output = (ROOT / args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    os.environ["COVERAGE_FILE"] = str(output / ".coverage")
    py = sys.executable
    run(py, "-m", "pip", "check")
    run(py, "-m", "ruff", "check", ".")
    run(py, "-m", "mypy", "--no-incremental", "--cache-dir", str(output / "mypy-cache"))
    node = shutil.which("node")
    if node is None:
        raise SystemExit("JavaScript validation requires Node.js; this is a development check, not a runtime dependency.")
    for source in sorted((ROOT / "dashboard/assets").glob("*.js")):
        run(node, "--check", str(source))
    run(py, "-X", "utf8", "-m", "coverage", "run", "-m", "pytest", "-q", "tests", "-p", "no:cacheprovider",
        "--junitxml=" + str(output / "tests.xml"))
    run(py, "-m", "coverage", "report")
    run(py, "-m", "coverage", "json", "-o", str(output / "coverage.json"))
    run(py, "scripts/check_coverage.py", str(output / "coverage.json"))


if __name__ == "__main__":
    main()
