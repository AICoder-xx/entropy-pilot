"""Versioned local wallet encryption, without plaintext temporary files.

Windows DPAPI uses the current logon user (never LOCAL_MACHINE).  Importing
this module is portable; unsupported hosts may read legacy files but cannot
silently save a new plaintext wallet.  Python secret strings remain in memory
while signing; this protects files at rest, not a compromised logon session.
"""
from __future__ import annotations

import base64
import binascii
import ctypes
import hashlib
import hmac
import json
import sys
from pathlib import Path


STORAGE_WINDOWS_DPAPI = "windows_dpapi"
STORAGE_LEGACY = "legacy_plaintext"
_ENVELOPE_FORMAT = "entropy-wallet-dpapi-v1"
_PAYLOAD_FORMAT = "entropy-wallet-payload-v1"
_ENTROPY = b"Entropy Trader wallet storage v1"
_UI_FORBIDDEN = 0x1
_RECOVERY_GUIDANCE = (
    "原文件保持不变；请先在保存时的 Windows 用户和电脑恢复读取。"
    "无法恢复时，请保留原密文，在新的独立安装/配置目录重新配置受限 API 钱包；"
    "不能在此不可读文件上直接覆盖导入。"
)


class CredentialStorageError(RuntimeError):
    """Actionable storage failure; messages must never contain secret input."""


class _DataBlob(ctypes.Structure):
    _fields_ = [("cbData", ctypes.c_uint32), ("pbData", ctypes.c_void_p)]


def secure_wallet_storage_available() -> bool:
    return sys.platform == "win32"


def _dpapi(data: bytes, *, protect: bool) -> bytes:
    if not secure_wallet_storage_available():
        if not protect:
            raise CredentialStorageError(
                "该钱包由 Windows 当前用户加密，当前系统无法解密。" + _RECOVERY_GUIDANCE
            )
        raise CredentialStorageError(
            "本机尚未支持安全钱包文件存储，已拒绝明文保存。请在 Windows 面板保存钱包；"
            "现有旧文件仍可读取，非 Windows 新凭证请由受控进程环境提供。"
        )
    # Windows-only ctypes attributes are absent from POSIX typing/runtime.
    # Resolve only after the supported-platform gate; never fall back to plaintext.
    load_windows_dll = getattr(ctypes, "WinDLL", None)
    if load_windows_dll is None:
        raise CredentialStorageError("本机 Windows 加密接口不可用，原钱包保持不变。")
    crypt32 = load_windows_dll("crypt32", use_last_error=True)
    kernel32 = load_windows_dll("kernel32", use_last_error=True)
    operation = crypt32.CryptProtectData if protect else crypt32.CryptUnprotectData
    operation.argtypes = [
        ctypes.POINTER(_DataBlob), ctypes.c_wchar_p if protect else ctypes.c_void_p,
        ctypes.POINTER(_DataBlob), ctypes.c_void_p, ctypes.c_void_p,
        ctypes.c_uint32, ctypes.POINTER(_DataBlob),
    ]
    operation.restype = ctypes.c_int
    kernel32.LocalFree.argtypes = [ctypes.c_void_p]
    kernel32.LocalFree.restype = ctypes.c_void_p
    source_buffer = ctypes.create_string_buffer(data)
    entropy_buffer = ctypes.create_string_buffer(_ENTROPY)
    source = _DataBlob(len(data), ctypes.addressof(source_buffer))
    entropy = _DataBlob(len(_ENTROPY), ctypes.addressof(entropy_buffer))
    output = _DataBlob()
    try:
        succeeded = operation(
            ctypes.byref(source), "Entropy Trader wallet" if protect else None,
            ctypes.byref(entropy), None, None, _UI_FORBIDDEN, ctypes.byref(output),
        )
        if not succeeded:
            if protect:
                raise CredentialStorageError(
                    "Windows 钱包加密失败，尚未替换原文件；"
                    "请检查当前 Windows 登录用户的系统加密服务后再保存。"
                )
            raise CredentialStorageError("钱包解密失败，可能是用户/电脑不匹配或文件损坏。" + _RECOVERY_GUIDANCE)
        return ctypes.string_at(output.pbData, output.cbData)
    finally:
        ctypes.memset(ctypes.addressof(source_buffer), 0, ctypes.sizeof(source_buffer))
        if output.pbData:
            ctypes.memset(output.pbData, 0, output.cbData)
            kernel32.LocalFree(output.pbData)


def _canonical_json(raw: dict) -> bytes:
    return json.dumps(raw, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def encode_wallet_document(raw: dict) -> dict:
    """Encrypt the entire document before it can reach an atomic temp file."""
    payload = {
        "format": _PAYLOAD_FORMAT,
        "sha256": hashlib.sha256(_canonical_json(raw)).hexdigest(),
        "wallet": raw,
    }
    encrypted = _dpapi(_canonical_json(payload), protect=True)
    return {"format": _ENVELOPE_FORMAT, "encrypted_data": base64.b64encode(encrypted).decode("ascii")}


def decode_wallet_document(raw: dict) -> tuple[dict, str]:
    if "encrypted_data" not in raw and "format" not in raw:
        return raw, STORAGE_LEGACY
    if set(raw) != {"format", "encrypted_data"} or raw.get("format") != _ENVELOPE_FORMAT:
        raise CredentialStorageError("钱包加密文件格式或版本不受支持。" + _RECOVERY_GUIDANCE)
    encoded = raw.get("encrypted_data")
    if not isinstance(encoded, str) or not encoded:
        raise CredentialStorageError("钱包加密数据格式无效。" + _RECOVERY_GUIDANCE)
    try:
        encrypted = base64.b64decode(encoded, validate=True)
    except (ValueError, binascii.Error):
        raise CredentialStorageError("钱包加密数据格式无效。" + _RECOVERY_GUIDANCE) from None
    plaintext = _dpapi(encrypted, protect=False)
    try:
        payload = json.loads(plaintext)
        wallet = payload["wallet"]
        checksum = payload["sha256"]
        if (
            not isinstance(wallet, dict) or payload.get("format") != _PAYLOAD_FORMAT
            or not isinstance(checksum, str)
            or not hmac.compare_digest(checksum, hashlib.sha256(_canonical_json(wallet)).hexdigest())
        ):
            raise ValueError("invalid protected payload")
    except (ValueError, TypeError, KeyError, UnicodeError):
        raise CredentialStorageError("钱包解密内容校验失败。" + _RECOVERY_GUIDANCE) from None
    return wallet, STORAGE_WINDOWS_DPAPI


def read_wallet_document(path: Path) -> tuple[dict | None, str]:
    if not path.exists():
        return None, "none"
    try:
        with path.open("r", encoding="utf-8") as handle:
            raw = json.load(handle)
    except (OSError, ValueError, UnicodeError):
        raise CredentialStorageError(f"无法读取私密钱包配置文件: {path.name}") from None
    if not isinstance(raw, dict):
        raise CredentialStorageError(f"私密钱包配置文件顶层必须是对象: {path.name}")
    return decode_wallet_document(raw)


def storage_message(storage: str) -> str:
    if storage == STORAGE_WINDOWS_DPAPI:
        return (
            "已使用 Windows 当前用户加密；更换电脑或登录用户时请保留原密文，"
            "在新的独立安装/配置目录重新配置受限 API 钱包。"
        )
    return "旧版明文钱包；在本机 Windows 面板点击保存即可加密，读取不会自动修改文件。"
