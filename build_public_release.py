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
    "main.py", "dashboard.py", "dashboard_setup.py", "dashboard_budget.py", "dashboard_charts.py", "analyze_efficiency.py", "audit_local_data.py",
    "audit_live.py", "recover_stopped_fills.py", "recover_stopped_orders.py", "build_public_release.py",
    "requirements.txt", "requirements-live.txt", "requirements-test.txt", "requirements-lock-2026-10-17.txt",
    "pytest.ini",
    "install.bat", "run_dashboard.bat", "run_market_maker.bat", "run_grid.bat", "check_market.bat",
    "config.example.json", "dashboard.config.example.json", "wallet.example.json", ".env.example", ".gitignore",
    "LICENSE", "THIRD_PARTY_NOTICES.md", "OPEN_SOURCE_AUDIT.md", "README_PUBLIC.md",
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


def build(output: Path):
    output = output.resolve()
    if output.exists():
        raise RuntimeError("输出目录已存在，请使用新目录；不会覆盖旧发布或用户数据")
    archive = output.with_suffix(".zip")
    if archive.exists():
        raise RuntimeError("ZIP 已存在，拒绝覆盖")
    output.mkdir(parents=True)
    paths = [Path(x) for x in FILES]
    paths += [p.relative_to(ROOT) for p in sorted((ROOT / "entropy_bot").glob("*.py"))]
    paths += [p.relative_to(ROOT) for p in sorted((ROOT / "tests").glob("test_*.py"))]
    for rel in paths:
        source = ROOT / rel
        if not source.is_file() or source.is_symlink():
            raise RuntimeError(f"发布源文件不存在或是链接: {rel}")
        target = output / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
    shutil.copyfile(ROOT / "README_PUBLIC.md", output / "README.md")
    base = json.loads((ROOT / "config.example.json").read_text(encoding="utf-8"))
    for name, strategy, mode in [("config.json", "market_maker", "paper"),
                                 ("config.market_maker.json", "market_maker", "paper"),
                                 ("config.grid.json", "grid", "paper"),
                                 ("config.live.stage-a.example.json", "market_maker", "live")]:
        payload = json.loads(json.dumps(base))
        payload.update(mode=mode, strategy=strategy)
        payload["runtime"].update(state_dir="state", log_dir="logs")
        # Dashboard setup uses the local GUI wallet, no hidden dotenv override.
        if mode == "live":
            payload["live"]["env_file"] = None
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
    (output / "PUBLIC_MANIFEST.json").write_text(json.dumps({
        "created_at": datetime.now().isoformat(), "files": manifest,
        "checks": {"allowlist_only": True, "no_runtime_data": True,
                   "secret_literal_scan": "passed", "tests_included_in_scan": True},
        "limits": "允许名单和静态规则不是所有可能秘密的通用检测器；测试中的密钥是合成夹具。"
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as bundle:
        for path in sorted(output.rglob("*")):
            if path.is_file():
                bundle.write(path, output.name + "/" + path.relative_to(output).as_posix())
    with zipfile.ZipFile(archive) as bundle:
        bad = bundle.testzip()
        if bad:
            raise RuntimeError("ZIP 校验失败: " + bad)
    return {"directory": str(output), "zip": str(archive), "files": len(manifest) + 1,
            "sha256": hashlib.sha256(archive.read_bytes()).hexdigest(), "zip_integrity": "passed"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default=str(ROOT / "releases" / ("entropy-pilot-" + datetime.now().strftime("%Y%m%d-%H%M%S"))))
    args = parser.parse_args()
    print(json.dumps(build(Path(args.output)), ensure_ascii=False, indent=2))
