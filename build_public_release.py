"""Build a public-source candidate from an explicit allowlist, never a working-tree ZIP."""
from __future__ import annotations
import argparse
import hashlib
import json
import re
import shutil
import zipfile
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
FILES = [
    "main.py", "dashboard.py", "dashboard_setup.py", "dashboard_budget.py", "dashboard_charts.py", "dashboard_http.py", "dashboard_recovery.py",
    "dashboard_snapshot.py", "dashboard_monitor.py", "analyze_efficiency.py", "audit_local_data.py",
    "audit_live.py", "recover_stopped_fills.py",
    "install.bat", "run_dashboard.bat", "run_market_maker.bat", "run_grid.bat", "check_market.bat",
    "dashboard.config.example.json", "wallet.example.json", ".env.example", ".gitignore",
    "LICENSE", "THIRD_PARTY_NOTICES.md", "CHANGELOG.md",
    "docs/ENGINEERING_QUALITY.md",
    "docs/ENGINEERING_BROKER.md", "docs/ENGINEERING_CONFIG_CREDENTIALS.md", "docs/ENGINEERING_RECOVERY.md",
    "dashboard/index.html",
    "docs/i18n/README.en.md", "docs/i18n/README.zh-TW.md",
    "docs/i18n/README.ja.md", "docs/i18n/README.ko.md",
]


def scan_public_text(relative_path: str, text: str) -> list[dict[str, str]]:
    """Apply the same credential checks to source, documentation and tests.

    Findings contain paths and categories only, never matched credential values.
    Synthetic test keys should be constructed in fixtures, not literal secrets.
    """
    findings = []
    patterns = {
        "literal_private_key": r'''["']?(?:private_key|secret_key|api_private_key)["']?\s*[:=]\s*["']?(?:0x)?[0-9a-fA-F]{64}(?![0-9a-fA-F])''',
        "personal_absolute_path": r'''[A-Za-z]:[/\\]+Users[/\\]+[^/\\\s"']+[/\\]+''',
    }
    for kind, pattern in patterns.items():
        if re.search(pattern, text, flags=re.IGNORECASE):
            findings.append({"file": relative_path, "kind": kind})
    return findings


ASSETS = [
    "common.js", "strategy.js", "metrics.js", "charts.js", "dashboard.js",
    "budget.js", "wallet.js", "events.js", "guide.js", "dashboard.css", "guide.css",
]


def build(output: Path, *, include_dev: bool = False):
    output = output.resolve()
    if output.exists():
        raise RuntimeError("输出目录已存在，请使用新目录；不会覆盖旧发布或用户数据")
    archive = output.with_suffix(".zip")
    if archive.exists():
        raise RuntimeError("ZIP 已存在，拒绝覆盖")
    output.mkdir(parents=True)
    paths = [Path(x) for x in FILES]
    paths += [Path("dashboard/assets") / name for name in ASSETS]
    paths += [p.relative_to(ROOT) for p in sorted((ROOT / "entropy_bot").glob("*.py"))]
    if include_dev:
        paths += [Path(name) for name in (
            "build_public_release.py", "recover_stopped_orders.py", "requirements-live.txt", "requirements-dev.txt", "pytest.ini", "pyproject.toml",
            ".prettierrc.json", ".github/workflows/quality.yml", "scripts/check_quality.py",
            "scripts/check_coverage.py",
        )]
        paths += [p.relative_to(ROOT) for p in sorted((ROOT / "tests").glob("*.py"))]
    for rel in paths:
        source = ROOT / rel
        if not source.is_file() or source.is_symlink():
            raise RuntimeError(f"发布源文件不存在或是链接: {rel}")
        target = output / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
    public_readme = ROOT / "README_PUBLIC.md"
    shutil.copyfile(public_readme if public_readme.is_file() else ROOT / "README.md", output / "README.md")
    requirements = (ROOT / "requirements.txt").read_text(encoding="utf-8").rstrip()
    live_requirements = "\n".join(line for line in
        (ROOT / "requirements-live.txt").read_text(encoding="utf-8").splitlines()
        if not line.strip().startswith("-r "))
    (output / "requirements.txt").write_text(requirements + "\n\n" + live_requirements.rstrip() + "\n", encoding="utf-8", newline="\n")
    if include_dev:
        dev = output / "requirements-dev.txt"
        dev.write_text(dev.read_text(encoding="utf-8").replace("-r requirements-live.txt", "-r requirements.txt"), encoding="utf-8")
        # Public tests must never rely on a user's mutable runtime configuration.
        for fixture in ("config.example.json", "config.market_maker.json", "config.live.stage-a.example.json"):
            shutil.copyfile(ROOT / "config.example.json", output / fixture)
    # Public installation needs one dependency list and one default paper config.
    for path in [*output.glob("*.py"), *output.glob("entropy_bot/*.py"), *output.glob("*.bat")]:
        data = path.read_bytes()
        if path.name != "build_public_release.py":
            data = data.replace(b"requirements-live.txt", b"requirements.txt")
        if path.name == "dashboard.py":
            data = data.replace(b"config.live.stage-a.example.json", b"config.live.example.json")
        if path.name == "run_market_maker.bat":
            data = data.replace(b"config.market_maker.json", b"config.json")
        path.write_bytes(data)
    env_example = output / ".env.example"
    env_example.write_bytes(env_example.read_bytes().replace(b"config.example.json", b"config.json"))
    base = json.loads((ROOT / "config.example.json").read_text(encoding="utf-8"))
    for name, strategy, mode in [("config.json", "market_maker", "paper"),
                                 ("config.grid.json", "grid", "paper"),
                                 ("config.live.example.json", "market_maker", "live")]:
        payload = json.loads(json.dumps(base))
        payload.update(mode=mode, strategy=strategy)
        payload["runtime"].update(state_dir="state", log_dir="logs")
        # Dashboard setup uses the local GUI wallet, no hidden dotenv override.
        if mode == "live":
            payload["live"]["env_file"] = None
            payload["_说明"] = "面板实盘模板，凭证为空。启动实盘会发送真实订单；请先配置已授权的 API 钱包并检查账户、预算、持仓与挂单。"
        (output / name).write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    # The dashboard reads the example until the user saves local settings.
    # Never ship or track mutable dashboard.config.json with a public repository.
    manifest, violations = [], []
    for path in sorted(output.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(output).as_posix()
        data = path.read_bytes()
        text = data.decode("utf-8")
        violations.extend(scan_public_text(rel, text))
        manifest.append({"path": rel, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()})
    if violations:
        raise RuntimeError("发布扫描未通过（仅文件名，不输出内容）: " + json.dumps(violations, ensure_ascii=False))
    manifest_path = output.with_name(output.name + "-manifest.json")
    if manifest_path.exists():
        raise RuntimeError("发布证据文件已存在，拒绝覆盖")
    manifest_path.write_text(json.dumps({
        "created_at": datetime.now().isoformat(), "files": manifest,
        "checks": {"allowlist_only": True, "no_runtime_data": True,
                   "secret_literal_scan": "passed", "all_published_files_scanned": True},
        "limits": "允许名单和静态规则不是所有可能秘密的通用检测器。"
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as bundle:
        for path in sorted(output.rglob("*")):
            if path.is_file():
                bundle.write(path, output.name + "/" + path.relative_to(output).as_posix())
    with zipfile.ZipFile(archive) as bundle:
        bad = bundle.testzip()
        if bad:
            raise RuntimeError("ZIP 校验失败: " + bad)
    return {"directory": str(output), "zip": str(archive), "files": len(manifest),
            "private_manifest": str(manifest_path),
            "sha256": hashlib.sha256(archive.read_bytes()).hexdigest(), "zip_integrity": "passed"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default=str(ROOT / "releases" / ("entropy-pilot-" + datetime.now().strftime("%Y%m%d-%H%M%S"))))
    parser.add_argument("--include-dev", action="store_true", help="Include offline tests, CI and quality tools in a reproducible source release")
    args = parser.parse_args()
    print(json.dumps(build(Path(args.output), include_dev=args.include_dev), ensure_ascii=False, indent=2))
