from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path

from .config import Config


_ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")
_WALLET_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,31}$")


def wallet_storage_slug(wallet_id: str) -> str:
    """Keep the established on-disk naming scheme; never silently rename it.

    Distinct display IDs must not resolve to the same runtime/config/ledger
    path, including on case-insensitive Windows filesystems.
    """
    return "".join(
        char.lower() if char.isalnum() else "_" for char in wallet_id
    ).strip("_") or "primary"


@dataclass(frozen=True)
class LiveCredentials:
    private_key: str | None = field(repr=False)
    account_address: str | None
    account_was_derived: bool
    source: str
    wallet_id: str


@dataclass(frozen=True)
class WalletEntry:
    wallet_id: str
    private_key: str | None = field(repr=False)
    account_address: str | None
    enabled: bool


def _load_private_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as handle:
            raw = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"无法读取私密钱包配置文件: {path.name}") from exc
    if not isinstance(raw, dict):
        raise RuntimeError(f"私密钱包配置文件顶层必须是对象: {path.name}")
    return raw


def _optional_text(value: object, field: str) -> str | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise RuntimeError(f"wallet.local.json 的 {field} 必须是字符串")
    return value.strip() or None


def read_wallet_entries(path: Path) -> list[WalletEntry]:
    """Read the ignored credentials file without exposing secrets to callers."""

    raw = _load_private_json(path)
    if raw is None:
        return []
    visible = {key for key in raw if not str(key).startswith("_")}
    if "wallets" not in visible:
        unknown = visible - {"private_key", "account_address"}
        if unknown:
            raise RuntimeError(f"私密钱包配置含未知字段: {sorted(unknown)}")
        return [
            WalletEntry(
                wallet_id="primary",
                private_key=_optional_text(raw.get("private_key"), "private_key"),
                account_address=_optional_text(
                    raw.get("account_address"), "account_address"
                ),
                enabled=True,
            )
        ]

    unknown = visible - {"wallets"}
    if unknown:
        raise RuntimeError(
            "多钱包格式不能再混用顶层 private_key/account_address"
        )
    wallets = raw.get("wallets")
    if not isinstance(wallets, list) or not wallets:
        raise RuntimeError("wallet.local.json 的 wallets 必须是非空数组")
    result: list[WalletEntry] = []
    seen: set[str] = set()
    seen_storage: set[str] = set()
    for index, item in enumerate(wallets, start=1):
        if not isinstance(item, dict):
            raise RuntimeError(f"wallets[{index}] 必须是对象")
        item_visible = {key for key in item if not str(key).startswith("_")}
        item_unknown = item_visible - {
            "id", "private_key", "account_address", "enabled"
        }
        if item_unknown:
            raise RuntimeError(
                f"wallets[{index}] 含未知字段: {sorted(item_unknown)}"
            )
        wallet_id = _optional_text(item.get("id"), f"wallets[{index}].id")
        if not wallet_id or not _WALLET_ID_RE.fullmatch(wallet_id):
            raise RuntimeError(
                f"wallets[{index}].id 只能使用字母、数字、下划线和连字符"
            )
        if wallet_id in seen:
            raise RuntimeError(f"钱包 id 重复: {wallet_id}")
        seen.add(wallet_id)
        storage_slug = wallet_storage_slug(wallet_id)
        if storage_slug == "primary" and wallet_id != "primary":
            raise RuntimeError("primary 是保留的钱包名称，不能使用大小写或下划线变体")
        if storage_slug in seen_storage:
            raise RuntimeError("钱包名称映射到相同本地路径，请使用不同名称（大小写、连字符和下划线不能用于区分）")
        seen_storage.add(storage_slug)
        enabled = item.get("enabled", True)
        if not isinstance(enabled, bool):
            raise RuntimeError(f"wallets[{index}].enabled 必须是布尔值")
        result.append(
            WalletEntry(
                wallet_id=wallet_id,
                private_key=_optional_text(
                    item.get("private_key"), f"wallets[{index}].private_key"
                ),
                account_address=_optional_text(
                    item.get("account_address"),
                    f"wallets[{index}].account_address",
                ),
                enabled=enabled,
            )
        )
    return result


def configured_wallet_ids(cfg: Config) -> list[str]:
    enabled = [
        entry
        for entry in read_wallet_entries(cfg.live.credentials_file)
        if entry.enabled
    ]
    # Two API keys aimed at the same funded account would make independent
    # market processes fight over one portfolio. Reject that topology before
    # the dashboard can start any child process.
    seen_accounts: set[str] = set()
    for entry in enabled:
        account = entry.account_address
        if not account and entry.private_key:
            try:
                from eth_account import Account

                account = Account.from_key(entry.private_key).address
            except (ImportError, ValueError, TypeError) as exc:
                raise RuntimeError(
                    f"钱包 {entry.wallet_id} 的私钥格式无效"
                ) from exc
        if account:
            normalized = account.lower()
            if normalized in seen_accounts:
                raise RuntimeError("多个启用项指向同一个资金账户，拒绝并行启动")
            seen_accounts.add(normalized)
    return [entry.wallet_id for entry in enabled]


def _read_private_file(cfg: Config) -> tuple[str | None, str | None, str]:
    entries = read_wallet_entries(cfg.live.credentials_file)
    if not entries:
        return None, None, cfg.live.wallet_id or "primary"
    selected_id = cfg.live.wallet_id
    enabled = [entry for entry in entries if entry.enabled]
    if selected_id is None:
        if len(enabled) > 1:
            raise RuntimeError(
                "检测到多个启用钱包；每个机器人配置必须指定 live.wallet_id"
            )
        selected = enabled[0] if enabled else None
    else:
        selected = next(
            (entry for entry in entries if entry.wallet_id == selected_id), None
        )
        # Compatibility for a dashboard that was already open when the user
        # imported their first named wallet.  Older/single-wallet sessions use
        # the implicit ``primary`` id; when exactly one enabled wallet exists,
        # that name is an unambiguous alias and should work immediately without
        # asking the user to close the panel or rename their wallet.
        if selected is None and selected_id == "primary" and len(enabled) == 1:
            selected = enabled[0]
    if selected is None:
        raise RuntimeError(f"未找到启用的钱包配置: {selected_id or 'default'}")
    if not selected.enabled:
        raise RuntimeError(f"钱包已禁用: {selected.wallet_id}")
    return selected.private_key, selected.account_address, selected.wallet_id


def _read_env_file(cfg: Config) -> dict[str, str | None]:
    """Read only the explicitly configured file, without mutating os.environ.

    Disabling interpolation avoids resolving ${...} from unrelated process
    variables.  Named-wallet processes never use this single-wallet source.
    """
    path = cfg.live.env_file
    if path is None or not path.exists():
        return {}
    try:
        from dotenv import dotenv_values

        return dict(dotenv_values(path, encoding="utf-8-sig", interpolate=False))
    except ImportError:
        raise RuntimeError("读取 .env 需要 python-dotenv；请重新运行 install.bat") from None
    except (OSError, UnicodeError):
        raise RuntimeError(f"无法读取本机环境配置文件: {path.name}") from None


def resolve_live_credentials(
    cfg: Config,
    *,
    require_private_key: bool,
) -> LiveCredentials:
    """Load secrets without ever logging or returning them in diagnostics.

    Environment variables override an explicitly configured .env file, then
    the ignored local JSON file. Named wallets remain isolated from both. If the
    account address is blank, the signer address is used, which is the simple
    main-wallet mode.  API wallets must specify the real master/subaccount
    address because the agent address has no account balances of its own.
    """

    file_key, file_address, wallet_id = _read_private_file(cfg)
    # Environment variables remain a backwards-compatible single-wallet
    # override. Explicit wallet selection must never make every child process
    # accidentally share the same environment signer.
    allow_environment = wallet_id == "primary"
    dotenv = _read_env_file(cfg) if allow_environment else {}
    env_key = (
        os.environ.get(cfg.live.private_key_env, "").strip()
        if allow_environment
        else ""
    )
    env_address = (
        os.environ.get(cfg.live.account_address_env, "").strip()
        if allow_environment
        else ""
    )
    dotenv_key = (dotenv.get(cfg.live.private_key_env) or "").strip()
    dotenv_address = (dotenv.get(cfg.live.account_address_env) or "").strip()
    private_key = env_key or dotenv_key or file_key
    account_address = env_address or dotenv_address or file_address

    sources: list[str] = []
    if env_key or env_address:
        sources.append("environment")
    if (not env_key and dotenv_key) or (not env_address and dotenv_address):
        sources.append(cfg.live.env_file.name)
    if (not env_key and not dotenv_key and file_key) or (not env_address and not dotenv_address and file_address):
        sources.append(cfg.live.credentials_file.name)
    source = "+".join(sources) if sources else "none"

    if require_private_key and not private_key:
        raise RuntimeError(
            f"实盘缺少私钥：请填写 {cfg.live.credentials_file.name} 的 private_key，"
            f"或设置环境变量 {cfg.live.private_key_env}（也支持 live.env_file 指定的 .env）"
        )

    derived = False
    if private_key:
        try:
            from eth_account import Account

            signer_address = Account.from_key(private_key).address
        except (ImportError, ValueError, TypeError) as exc:
            raise RuntimeError("钱包私钥格式无效") from exc
        if not account_address:
            account_address = signer_address
            derived = True

    if account_address and not _ADDRESS_RE.fullmatch(account_address):
        raise RuntimeError("Hyperliquid 主账户/子账户地址格式不正确")

    return LiveCredentials(
        private_key=private_key,
        account_address=account_address.lower() if account_address else None,
        account_was_derived=derived,
        source=source,
        wallet_id=wallet_id,
    )
