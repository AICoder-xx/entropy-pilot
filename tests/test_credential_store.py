"""Credential-at-rest acceptance uses synthetic accounts and temporary files."""
import base64
import json
import secrets
import sys
from dataclasses import replace
from pathlib import Path

import pytest
from eth_account import Account

import dashboard_setup
from entropy_bot import credential_store, storage
from entropy_bot.config import load_config
from entropy_bot.credential_store import CredentialStorageError
from entropy_bot.credentials import read_wallet_entries, resolve_live_credentials


@pytest.fixture
def wallet_payload():
    return {
        "wallet_id": "synthetic",
        "private_key": Account.create().key.hex(),
        "account_address": Account.create().address,
    }


@pytest.fixture
def portable_cipher(monkeypatch):
    """Opaque in-memory tokens exercise portable persistence, not encryption."""
    encrypted = {}

    def transform(data, *, protect):
        if protect:
            token = secrets.token_bytes(32)
            encrypted[token] = data
            return token
        if data not in encrypted:
            raise CredentialStorageError("synthetic corrupted ciphertext")
        return encrypted[data]

    monkeypatch.setattr(credential_store, "_dpapi", transform)


@pytest.mark.skipif(sys.platform != "win32", reason="requires real Windows current-user DPAPI")
def test_real_dpapi_roundtrip_randomization_and_tamper_rejection():
    raw = {"wallets": [{"id": "synthetic", "private_key": secrets.token_hex(32)}]}
    first = credential_store.encode_wallet_document(raw)
    second = credential_store.encode_wallet_document(raw)
    assert first != second
    assert raw["wallets"][0]["private_key"] not in json.dumps(first)
    assert credential_store.decode_wallet_document(first) == (raw, "windows_dpapi")
    encrypted = bytearray(base64.b64decode(first["encrypted_data"]))
    encrypted[-1] ^= 0x01
    first["encrypted_data"] = base64.b64encode(encrypted).decode("ascii")
    with pytest.raises(CredentialStorageError):
        credential_store.decode_wallet_document(first)


def test_missing_native_encryption_api_preserves_original_wallet(tmp_path, wallet_payload, monkeypatch):
    path = tmp_path / "wallet.local.json"
    original = json.dumps({"wallets": [{"id": "synthetic", **{
        key: value for key, value in wallet_payload.items() if key != "wallet_id"
    }}]}).encode()
    path.write_bytes(original)
    monkeypatch.setattr(credential_store, "secure_wallet_storage_available", lambda: True)
    monkeypatch.setattr(credential_store.ctypes, "WinDLL", None, raising=False)
    with pytest.raises(CredentialStorageError, match="加密接口不可用"):
        dashboard_setup.save_wallet(path, wallet_payload)
    assert path.read_bytes() == original
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.skipif(sys.platform != "win32", reason="requires real Windows current-user DPAPI")
def test_real_saved_wallet_resolves_credentials_without_read_side_migration(tmp_path, wallet_payload, monkeypatch):
    path = tmp_path / "wallet.local.json"
    dashboard_setup.save_wallet(path, wallet_payload)
    before = path.read_bytes()
    cfg = load_config(Path(__file__).parents[1] / "config.example.json")
    cfg = replace(cfg, live=replace(cfg.live, credentials_file=path, wallet_id="synthetic", env_file=None))
    monkeypatch.setenv(cfg.live.private_key_env, Account.create().key.hex())
    result = resolve_live_credentials(cfg, require_private_key=True)
    assert result.private_key == wallet_payload["private_key"]
    assert result.account_address == wallet_payload["account_address"].lower()
    assert result.wallet_id == "synthetic"
    assert path.read_bytes() == before


@pytest.mark.parametrize("multi", [False, True])
def test_legacy_read_does_not_write_and_save_encrypts_every_entry(tmp_path, wallet_payload, portable_cipher, multi):
    path = tmp_path / "wallet.local.json"
    old = {"private_key": wallet_payload["private_key"], "account_address": wallet_payload["account_address"]}
    raw = {"_note": "keep metadata", **old}
    wallet_payload["wallet_id"] = "primary"
    if multi:
        raw = {"_note": "keep metadata", "wallets": [
            {"id": "primary", **old, "enabled": True, "_memo": "keep row metadata"},
            {"id": "disabled", "private_key": Account.create().key.hex(), "enabled": False},
        ]}
    path.write_text(json.dumps(raw), encoding="utf-8")
    before = path.read_bytes()
    assert all(e.storage == "legacy_plaintext" for e in read_wallet_entries(path))
    assert not dashboard_setup.wallet_status(path)[0]["storage_encrypted"]
    assert path.read_bytes() == before
    dashboard_setup.save_wallet(path, {"wallet_id": "primary"})
    protected, kind = credential_store.read_wallet_document(path)
    assert kind == "windows_dpapi"
    assert protected["_note"] == "keep metadata"
    assert protected["wallets"][0]["private_key"] == wallet_payload["private_key"]
    if multi:
        assert protected["wallets"][0]["_memo"] == "keep row metadata"
        assert protected["wallets"][1] == raw["wallets"][1]
    disk = path.read_text(encoding="utf-8")
    for entry in protected["wallets"]:
        assert entry["private_key"] not in disk
    assert list(tmp_path.iterdir()) == [path]


def test_atomic_replace_failure_keeps_legacy_file_and_temp_never_contains_secrets(tmp_path, wallet_payload, portable_cipher, monkeypatch):
    path = tmp_path / "wallet.local.json"
    legacy = {"wallets": [{"id": "synthetic", "private_key": wallet_payload["private_key"],
                            "account_address": wallet_payload["account_address"]}]}
    path.write_text(json.dumps(legacy), encoding="utf-8")
    before = path.read_bytes()
    observed = []

    def fail_replace(temp, target):
        data = Path(temp).read_bytes()
        assert wallet_payload["private_key"].encode() not in data
        assert wallet_payload["account_address"].lower().encode() not in data
        observed.append(data)
        raise PermissionError("synthetic replace denial")

    monkeypatch.setattr(storage.os, "replace", fail_replace)
    monkeypatch.setattr(storage.time, "sleep", lambda _delay: None)
    with pytest.raises(PermissionError):
        dashboard_setup.save_wallet(path, wallet_payload)
    assert len(observed) == 6
    assert path.read_bytes() == before
    assert list(tmp_path.iterdir()) == [path]


def test_encryption_failure_leaves_file_and_directory_untouched(tmp_path, wallet_payload, monkeypatch):
    path = tmp_path / "wallet.local.json"
    path.write_text(json.dumps({"wallets": [{"id": "synthetic", "private_key": wallet_payload["private_key"],
                                           "account_address": wallet_payload["account_address"]}]}), encoding="utf-8")
    before = path.read_bytes()

    def fail_encrypt(*args, **kwargs):
        raise CredentialStorageError("synthetic encryption failure")

    monkeypatch.setattr(credential_store, "_dpapi", fail_encrypt)
    with pytest.raises(CredentialStorageError):
        dashboard_setup.save_wallet(path, wallet_payload)
    assert path.read_bytes() == before
    assert list(tmp_path.iterdir()) == [path]


def test_unsupported_host_refuses_new_plaintext_file(tmp_path, wallet_payload, monkeypatch):
    monkeypatch.setattr(credential_store, "secure_wallet_storage_available", lambda: False)
    path = tmp_path / "wallet.local.json"
    with pytest.raises(CredentialStorageError, match="拒绝明文保存"):
        dashboard_setup.save_wallet(path, wallet_payload)
    assert not path.exists()
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("envelope", [
    {"format": "entropy-wallet-dpapi-v2", "encrypted_data": "abc"},
    {"format": "entropy-wallet-dpapi-v1", "encrypted_data": "!invalid-base64!"},
    {"format": "entropy-wallet-dpapi-v1", "encrypted_data": "", "wallets": []},
    {"encrypted_data": ""},
])
def test_corrupt_or_newer_envelope_never_falls_back_to_plaintext(envelope):
    with pytest.raises(CredentialStorageError):
        credential_store.decode_wallet_document(envelope)


@pytest.mark.parametrize("plaintext", [b"not-json", b"null", b"[]", b'{"wallet":{},"sha256":"wrong","format":"entropy-wallet-payload-v1"}'])
def test_decryption_success_still_requires_valid_payload_and_checksum(monkeypatch, plaintext):
    monkeypatch.setattr(credential_store, "_dpapi", lambda *args, **kwargs: plaintext)
    with pytest.raises(CredentialStorageError, match="内容校验失败"):
        credential_store.decode_wallet_document({"format": "entropy-wallet-dpapi-v1", "encrypted_data": "YQ=="})
