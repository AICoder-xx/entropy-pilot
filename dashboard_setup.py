"""Local wallet setup. Never return credentials to the browser."""
from __future__ import annotations

import json
import os
import re
from pathlib import Path

from entropy_bot.credentials import read_wallet_entries, wallet_storage_slug
from entropy_bot.storage import atomic_write_json


def wallet_status(path: Path) -> list[dict]:
    return [{"id": e.wallet_id, "account_address_masked":
             (e.account_address[:6] + "…" + e.account_address[-4:]) if e.account_address else "未填写",
             "has_private_key": bool(e.private_key), "enabled": e.enabled}
            for e in read_wallet_entries(path)]


def save_wallet(path: Path, payload: dict) -> None:
    if not isinstance(payload, dict) or set(payload) - {"wallet_id", "account_address", "private_key"}:
        raise ValueError("配置字段无效")
    for key in ("wallet_id", "account_address", "private_key"):
        if not isinstance(payload.get(key, ""), str):
            raise ValueError("配置字段必须为文字")
    wallet_id = payload.get("wallet_id", "primary").strip() or "primary"
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,31}", wallet_id):
        raise ValueError("钱包名称只允许 1–32 位字母、数字、下划线或连字符")
    if wallet_storage_slug(wallet_id) == "primary" and wallet_id != "primary":
        raise ValueError("primary 是保留名称，请使用原拼写或其他钱包名称")
    entries = read_wallet_entries(path)
    if any(e.wallet_id != wallet_id and wallet_storage_slug(e.wallet_id) == wallet_storage_slug(wallet_id) for e in entries):
        raise ValueError("钱包名称与已有目录冲突，请勿只改大小写、连字符或下划线")
    if wallet_id != "primary":
        entries = [e for e in entries if not (
            e.wallet_id == "primary" and not e.private_key and not e.account_address)]
    existing = next((e for e in entries if e.wallet_id == wallet_id), None)
    key = payload.get("private_key", "").strip() or (existing.private_key if existing else None)
    account = payload.get("account_address", "").strip() or (existing.account_address if existing else None)
    if not account or not re.fullmatch(r"0x[0-9a-fA-F]{40}", account):
        raise ValueError("请填写有效的资金主账户或子账户地址")
    if not key:
        raise ValueError("首次配置请填写 API 钱包私钥")
    try:
        from eth_account import Account
        signer = Account.from_key(key).address
    except (ValueError, TypeError):
        raise ValueError("API 私钥格式无效") from None
    if signer.lower() == account.lower():
        raise ValueError("请使用独立 API 钱包私钥，并填写资金账户地址")
    old_account = existing.account_address if existing else None
    if existing and not old_account and existing.private_key:
        try:
            old_account = Account.from_key(existing.private_key).address
        except (ValueError, TypeError):
            raise ValueError("旧钱包身份无法核实，请使用新的钱包名称") from None
    if old_account and old_account.lower() != account.lower():
        raise ValueError("不同资金账户请使用新的钱包名称，避免复用旧订单与库存台账")
    for other in entries:
        if other.wallet_id == wallet_id or not other.enabled:
            continue
        other_account = other.account_address
        if not other_account and other.private_key:
            try:
                other_account = Account.from_key(other.private_key).address
            except (ValueError, TypeError):
                raise ValueError("已有钱包身份无法核实，请先修复配置") from None
        if other_account and other_account.lower() == account.lower():
            raise ValueError("该资金账户已存在，请更新原钱包名称，不能重复启用")
    # Preserve unrelated fields and existing wallet entries. No secret backups.
    raw = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    if "wallets" not in raw:
        notes = {k: v for k, v in raw.items() if k.startswith("_")}
        raw = {**notes, "wallets": [
            {"id": e.wallet_id, "private_key": e.private_key,
             "account_address": e.account_address, "enabled": e.enabled} for e in entries]}
    elif wallet_id != "primary":
        raw["wallets"] = [e for e in raw["wallets"] if not (
            e.get("id") == "primary" and not e.get("private_key") and not e.get("account_address"))]
    target = next((e for e in raw["wallets"] if e.get("id") == wallet_id), None)
    if target is None:
        target = {"id": wallet_id, "enabled": True}
        raw["wallets"].append(target)
    target.update(private_key=key, account_address=account.lower())
    atomic_write_json(path, raw)
    if os.name != "nt":
        path.chmod(0o600)
