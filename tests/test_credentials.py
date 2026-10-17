import json
from dataclasses import replace
from pathlib import Path

from eth_account import Account
import pytest

from entropy_bot.config import load_config
from entropy_bot.credentials import configured_wallet_ids, read_wallet_entries, resolve_live_credentials, wallet_storage_slug


def test_local_main_wallet_key_derives_account_address(tmp_path, monkeypatch):
    cfg = load_config(Path(__file__).parents[1] / "config.live.stage-a.example.json")
    wallet = Account.create()
    credentials_path = tmp_path / "wallet.local.json"
    credentials_path.write_text(
        json.dumps({"private_key": wallet.key.hex(), "account_address": ""}),
        encoding="utf-8",
    )
    cfg = replace(cfg, live=replace(cfg.live, credentials_file=credentials_path))
    monkeypatch.delenv(cfg.live.private_key_env, raising=False)
    monkeypatch.delenv(cfg.live.account_address_env, raising=False)

    credentials = resolve_live_credentials(cfg, require_private_key=True)

    assert credentials.account_address == wallet.address.lower()
    assert credentials.account_was_derived is True
    assert credentials.source == "wallet.local.json"


def test_environment_can_override_local_api_wallet_account(tmp_path, monkeypatch):
    cfg = load_config(Path(__file__).parents[1] / "config.live.stage-a.example.json")
    file_wallet = Account.create()
    env_wallet = Account.create()
    target = "0x" + "7" * 40
    credentials_path = tmp_path / "wallet.local.json"
    credentials_path.write_text(
        json.dumps(
            {"private_key": file_wallet.key.hex(), "account_address": "0x" + "6" * 40}
        ),
        encoding="utf-8",
    )
    cfg = replace(cfg, live=replace(cfg.live, credentials_file=credentials_path))
    monkeypatch.setenv(cfg.live.private_key_env, env_wallet.key.hex())
    monkeypatch.setenv(cfg.live.account_address_env, target)

    credentials = resolve_live_credentials(cfg, require_private_key=True)

    assert credentials.private_key == env_wallet.key.hex()
    assert credentials.account_address == target
    assert credentials.account_was_derived is False
    assert credentials.source == "environment"


def test_multiple_wallets_require_and_honor_explicit_wallet_id(tmp_path, monkeypatch):
    cfg = load_config(Path(__file__).parents[1] / "config.live.stage-a.example.json")
    first = Account.create()
    second = Account.create()
    credentials_path = tmp_path / "wallet.local.json"
    credentials_path.write_text(
        json.dumps(
            {
                "wallets": [
                    {
                        "id": "wallet1",
                        "enabled": True,
                        "private_key": first.key.hex(),
                        "account_address": "",
                    },
                    {
                        "id": "wallet2",
                        "enabled": True,
                        "private_key": second.key.hex(),
                        "account_address": "",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    cfg = replace(cfg, live=replace(cfg.live, credentials_file=credentials_path))
    monkeypatch.delenv(cfg.live.private_key_env, raising=False)
    monkeypatch.delenv(cfg.live.account_address_env, raising=False)

    assert configured_wallet_ids(cfg) == ["wallet1", "wallet2"]
    try:
        resolve_live_credentials(cfg, require_private_key=True)
    except RuntimeError as exc:
        assert "live.wallet_id" in str(exc)
    else:
        raise AssertionError("multiple wallets must require an explicit selector")

    selected = replace(cfg, live=replace(cfg.live, wallet_id="wallet2"))
    credentials = resolve_live_credentials(selected, require_private_key=True)
    assert credentials.wallet_id == "wallet2"
    assert credentials.account_address == second.address.lower()
    assert credentials.private_key == second.key.hex()


def test_primary_alias_uses_only_enabled_named_wallet(tmp_path, monkeypatch):
    cfg = load_config(Path(__file__).parents[1] / "config.live.stage-a.example.json")
    wallet = Account.create()
    credentials_path = tmp_path / "wallet.local.json"
    credentials_path.write_text(
        json.dumps(
            {
                "wallets": [
                    {
                        "id": "wallet1",
                        "enabled": True,
                        "private_key": wallet.key.hex(),
                        "account_address": "",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    cfg = replace(
        cfg,
        live=replace(
            cfg.live,
            credentials_file=credentials_path,
            wallet_id="primary",
        ),
    )
    monkeypatch.delenv(cfg.live.private_key_env, raising=False)
    monkeypatch.delenv(cfg.live.account_address_env, raising=False)

    credentials = resolve_live_credentials(cfg, require_private_key=True)

    assert credentials.wallet_id == "wallet1"
    assert credentials.account_address == wallet.address.lower()


def test_disabled_wallet_is_not_returned_for_parallel_start(tmp_path):
    cfg = load_config(Path(__file__).parents[1] / "config.live.stage-a.example.json")
    credentials_path = tmp_path / "wallet.local.json"
    credentials_path.write_text(
        json.dumps(
            {
                "wallets": [
                    {"id": "active", "enabled": True, "private_key": ""},
                    {"id": "off", "enabled": False, "private_key": ""},
                ]
            }
        ),
        encoding="utf-8",
    )
    cfg = replace(cfg, live=replace(cfg.live, credentials_file=credentials_path))
    assert configured_wallet_ids(cfg) == ["active"]


def test_parallel_start_rejects_two_api_keys_for_same_funded_account(tmp_path):
    cfg = load_config(Path(__file__).parents[1] / "config.live.stage-a.example.json")
    funded = "0x" + "7" * 40
    credentials_path = tmp_path / "wallet.local.json"
    credentials_path.write_text(
        json.dumps(
            {
                "wallets": [
                    {"id": "agent1", "private_key": Account.create().key.hex(), "account_address": funded},
                    {"id": "agent2", "private_key": Account.create().key.hex(), "account_address": funded},
                ]
            }
        ),
        encoding="utf-8",
    )
    cfg = replace(cfg, live=replace(cfg.live, credentials_file=credentials_path))
    try:
        configured_wallet_ids(cfg)
    except RuntimeError as exc:
        assert "同一个资金账户" in str(exc)
    else:
        raise AssertionError("duplicate funded account must be rejected")


def test_explicit_dotenv_is_loaded_without_process_environment_mutation(tmp_path, monkeypatch):
    cfg = load_config(Path(__file__).parents[1] / "config.example.json")
    api = Account.create()
    address = "0x" + "7" * 40
    env_file = tmp_path / ".env"
    env_file.write_text(
        f'{cfg.live.private_key_env}="{api.key.hex()}"\n{cfg.live.account_address_env}={address}\n',
        encoding="utf-8-sig",
    )
    cfg = replace(cfg, live=replace(cfg.live, env_file=env_file, credentials_file=tmp_path / "absent.json"))
    monkeypatch.delenv(cfg.live.private_key_env, raising=False)
    monkeypatch.delenv(cfg.live.account_address_env, raising=False)

    credentials = resolve_live_credentials(cfg, require_private_key=True)

    assert credentials.private_key == api.key.hex()
    assert credentials.account_address == address
    assert credentials.source == ".env"
    import os
    assert cfg.live.private_key_env not in os.environ
    assert cfg.live.account_address_env not in os.environ


def test_environment_overrides_explicit_dotenv(tmp_path, monkeypatch):
    cfg = load_config(Path(__file__).parents[1] / "config.example.json")
    file_api, env_api = Account.create(), Account.create()
    env_file = tmp_path / ".env"
    env_file.write_text(
        f'{cfg.live.private_key_env}={file_api.key.hex()}\n{cfg.live.account_address_env}=0x' + '6' * 40,
        encoding="utf-8",
    )
    cfg = replace(cfg, live=replace(cfg.live, env_file=env_file, credentials_file=tmp_path / "absent.json"))
    monkeypatch.setenv(cfg.live.private_key_env, env_api.key.hex())
    monkeypatch.setenv(cfg.live.account_address_env, "0x" + "7" * 40)

    credentials = resolve_live_credentials(cfg, require_private_key=True)

    assert credentials.private_key == env_api.key.hex()
    assert credentials.account_address == "0x" + "7" * 40
    assert credentials.source == "environment"


def test_primary_alias_does_not_override_named_wallet_with_environment(tmp_path, monkeypatch):
    cfg = load_config(Path(__file__).parents[1] / "config.example.json")
    named, global_api = Account.create(), Account.create()
    wallet_file = tmp_path / "wallet.local.json"
    wallet_file.write_text(json.dumps({"wallets": [{"id": "named", "private_key": named.key.hex()}]}), encoding="utf-8")
    env_file = tmp_path / ".env"
    env_file.write_text(f'{cfg.live.private_key_env}={global_api.key.hex()}', encoding="utf-8")
    cfg = replace(cfg, live=replace(cfg.live, credentials_file=wallet_file, wallet_id="primary", env_file=env_file))
    monkeypatch.setenv(cfg.live.private_key_env, global_api.key.hex())
    monkeypatch.setenv(cfg.live.account_address_env, global_api.address)

    credentials = resolve_live_credentials(cfg, require_private_key=True)

    assert credentials.wallet_id == "named"
    assert credentials.private_key == named.key.hex()
    assert credentials.account_address == named.address.lower()
    assert credentials.source == "wallet.local.json"


def test_secret_dataclass_repr_does_not_contain_private_key(tmp_path, monkeypatch):
    cfg = load_config(Path(__file__).parents[1] / "config.example.json")
    wallet = Account.create()
    wallet_file = tmp_path / "wallet.local.json"
    wallet_file.write_text(json.dumps({"private_key": wallet.key.hex()}), encoding="utf-8")
    cfg = replace(cfg, live=replace(cfg.live, credentials_file=wallet_file, env_file=None))
    monkeypatch.delenv(cfg.live.private_key_env, raising=False)
    monkeypatch.delenv(cfg.live.account_address_env, raising=False)

    assert wallet.key.hex() not in repr(resolve_live_credentials(cfg, require_private_key=True))
    assert wallet.key.hex() not in repr(read_wallet_entries(wallet_file))


def test_dotenv_requires_explicit_config_opt_in(tmp_path, monkeypatch):
    cfg = load_config(Path(__file__).parents[1] / "config.live.stage-a.example.json")
    wallet = Account.create()
    (tmp_path / ".env").write_text(f'{cfg.live.private_key_env}={wallet.key.hex()}', encoding="utf-8")
    cfg = replace(cfg, live=replace(cfg.live, credentials_file=tmp_path / "absent.json"))
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv(cfg.live.private_key_env, raising=False)
    monkeypatch.delenv(cfg.live.account_address_env, raising=False)

    credentials = resolve_live_credentials(cfg, require_private_key=False)

    assert credentials.private_key is None
    assert credentials.source == "none"


@pytest.mark.parametrize("first,second", [("desk-a", "desk_a"), ("Desk", "desk"), ("desk", "desk_")])
@pytest.mark.parametrize("second_enabled", [True, False])
def test_wallet_ids_cannot_share_storage_paths_even_when_disabled(tmp_path, first, second, second_enabled):
    path = tmp_path / "wallet.local.json"
    path.write_text(json.dumps({"wallets": [
        {"id": first, "enabled": True, "private_key": ""},
        {"id": second, "enabled": second_enabled, "private_key": ""},
    ]}), encoding="utf-8")
    original = path.read_bytes()

    assert wallet_storage_slug(first) == wallet_storage_slug(second)
    with pytest.raises(RuntimeError, match="相同本地路径"):
        read_wallet_entries(path)
    assert path.read_bytes() == original


@pytest.mark.parametrize("wallet_id", ["Primary", "PRIMARY", "primary_", "primary-"])
def test_primary_storage_alias_is_reserved_even_for_one_wallet(tmp_path, wallet_id):
    path = tmp_path / "wallet.local.json"
    path.write_text(json.dumps({"wallets": [{"id": wallet_id, "private_key": ""}]}), encoding="utf-8")
    with pytest.raises(RuntimeError, match="保留"):
        read_wallet_entries(path)
