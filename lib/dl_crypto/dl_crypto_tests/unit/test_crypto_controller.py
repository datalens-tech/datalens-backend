import pydantic
import pytest

from dl_configs.crypto_keys import CryptoKeysConfig
from dl_crypto import (
    CryptoController,
    CryptoKeysSettings,
    generate_fernet_key,
)
import dl_settings


@pytest.fixture(name="single_key_settings")
def fixture_single_key_settings(monkeypatch: pytest.MonkeyPatch) -> CryptoKeysSettings:
    class _RootSettings(dl_settings.BaseRootSettings):
        CRYPTO: CryptoKeysSettings = NotImplemented

    monkeypatch.setenv("CRYPTO__MAP_ID_KEY__KEY1", generate_fernet_key())
    monkeypatch.setenv("CRYPTO__ACTUAL_KEY_ID", "KEY1")
    return _RootSettings().CRYPTO


@pytest.fixture(name="dual_key_settings")
def fixture_dual_key_settings() -> CryptoKeysSettings:
    return CryptoKeysSettings(
        MAP_ID_KEY={
            "OLD": pydantic.SecretStr(generate_fernet_key()),
            "NEW": pydantic.SecretStr(generate_fernet_key()),
        },
        ACTUAL_KEY_ID="NEW",
    )


def test_encrypt_returns_encrypted_data_structure(single_key_settings: CryptoKeysSettings) -> None:
    controller = CryptoController(single_key_settings)
    encrypted = controller.encrypt_with_actual_key("my_secret")
    assert encrypted is not None
    assert encrypted["key_id"] == "KEY1"
    assert encrypted["key_kind"] == "local_fernet"
    assert encrypted["cypher_text"] != "my_secret"


def test_decrypt_roundtrip(single_key_settings: CryptoKeysSettings) -> None:
    controller = CryptoController(single_key_settings)
    encrypted = controller.encrypt_with_actual_key("my_secret")
    assert controller.decrypt(encrypted) == "my_secret"


def test_encrypt_none_returns_none(single_key_settings: CryptoKeysSettings) -> None:
    controller = CryptoController(single_key_settings)
    assert controller.encrypt_with_actual_key(None) is None


def test_decrypt_none_returns_none(single_key_settings: CryptoKeysSettings) -> None:
    controller = CryptoController(single_key_settings)
    assert controller.decrypt(None) is None


def test_cypher_text_differs_from_plain_text(single_key_settings: CryptoKeysSettings) -> None:
    controller = CryptoController(single_key_settings)
    plain = "secret_value"
    encrypted = controller.encrypt_with_actual_key(plain)
    assert encrypted is not None
    assert encrypted["cypher_text"] != plain


def test_encrypt_with_specific_key_id(dual_key_settings: CryptoKeysSettings) -> None:
    controller = CryptoController(dual_key_settings)
    encrypted = controller.encrypt("OLD", "secret")
    assert encrypted is not None
    assert encrypted["key_id"] == "OLD"


def test_encrypt_with_actual_key_uses_actual_key_id(dual_key_settings: CryptoKeysSettings) -> None:
    controller = CryptoController(dual_key_settings)
    encrypted = controller.encrypt_with_actual_key("secret")
    assert encrypted is not None
    assert encrypted["key_id"] == "NEW"


def test_decrypt_data_encrypted_with_old_key(dual_key_settings: CryptoKeysSettings) -> None:
    controller = CryptoController(dual_key_settings)
    encrypted_with_old = controller.encrypt("OLD", "old_secret")
    assert controller.decrypt(encrypted_with_old) == "old_secret"


def test_decrypt_data_encrypted_with_new_key(dual_key_settings: CryptoKeysSettings) -> None:
    controller = CryptoController(dual_key_settings)
    encrypted_with_new = controller.encrypt_with_actual_key("new_secret")
    assert controller.decrypt(encrypted_with_new) == "new_secret"


def test_actual_key_id_property(dual_key_settings: CryptoKeysSettings) -> None:
    controller = CryptoController(dual_key_settings)
    assert controller.actual_key_id == "NEW"


def test_crypto_controller_built_from_legacy_crypto_keys_config() -> None:
    key_val = generate_fernet_key()
    config = CryptoKeysConfig(  # type: ignore[call-arg]
        map_id_key={"KEY1": key_val},
        actual_key_id="KEY1",
    )
    controller = CryptoController(config)
    encrypted = controller.encrypt_with_actual_key("secret")
    assert controller.decrypt(encrypted) == "secret"


def test_crypto_keys_settings_exposes_protocol_properties() -> None:
    key_val = generate_fernet_key()
    settings = CryptoKeysSettings(MAP_ID_KEY={"KEY1": pydantic.SecretStr(key_val)}, ACTUAL_KEY_ID="KEY1")
    assert settings.map_id_key == {"KEY1": key_val}
    assert settings.actual_key_id == "KEY1"


def test_crypto_controller_created_directly_from_settings(single_key_settings: CryptoKeysSettings) -> None:
    controller = CryptoController(single_key_settings)
    encrypted = controller.encrypt_with_actual_key("secret")
    assert controller.decrypt(encrypted) == "secret"


def test_crypto_keys_settings_stores_keys_and_actual_key_id() -> None:
    key_val = generate_fernet_key()
    settings = CryptoKeysSettings(MAP_ID_KEY={"KEY1": pydantic.SecretStr(key_val)}, ACTUAL_KEY_ID="KEY1")
    assert settings.ACTUAL_KEY_ID == "KEY1"
    assert settings.MAP_ID_KEY == {"KEY1": pydantic.SecretStr(key_val)}


def test_crypto_keys_settings_loaded_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    class _RootSettings(dl_settings.BaseRootSettings):
        CRYPTO: CryptoKeysSettings = NotImplemented

    key_val = generate_fernet_key()
    monkeypatch.setenv("CRYPTO__MAP_ID_KEY__KEY1", key_val)
    monkeypatch.setenv("CRYPTO__ACTUAL_KEY_ID", "KEY1")

    settings = _RootSettings().CRYPTO
    assert settings.ACTUAL_KEY_ID == "KEY1"
    assert settings.MAP_ID_KEY == {"KEY1": pydantic.SecretStr(key_val)}


def test_crypto_keys_settings_loaded_from_env_with_two_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    class _RootSettings(dl_settings.BaseRootSettings):
        CRYPTO: CryptoKeysSettings = NotImplemented

    old_val = generate_fernet_key()
    new_val = generate_fernet_key()
    monkeypatch.setenv("CRYPTO__MAP_ID_KEY__OLD", old_val)
    monkeypatch.setenv("CRYPTO__MAP_ID_KEY__NEW", new_val)
    monkeypatch.setenv("CRYPTO__ACTUAL_KEY_ID", "NEW")

    settings = _RootSettings().CRYPTO
    assert settings.ACTUAL_KEY_ID == "NEW"
    assert settings.MAP_ID_KEY == {"OLD": pydantic.SecretStr(old_val), "NEW": pydantic.SecretStr(new_val)}


def test_crypto_keys_settings_key_values_hidden_in_repr() -> None:
    key_val = generate_fernet_key()
    settings = CryptoKeysSettings(MAP_ID_KEY={"KEY1": pydantic.SecretStr(key_val)}, ACTUAL_KEY_ID="KEY1")
    assert key_val not in repr(settings)
    assert key_val not in repr(settings.MAP_ID_KEY)


def test_crypto_keys_settings_actual_key_id_not_in_map_raises() -> None:
    key_val = generate_fernet_key()
    with pytest.raises(pydantic.ValidationError):
        CryptoKeysSettings(MAP_ID_KEY={"KEY1": pydantic.SecretStr(key_val)}, ACTUAL_KEY_ID="MISSING")
