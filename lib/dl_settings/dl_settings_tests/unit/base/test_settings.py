import pydantic
import pytest

import dl_settings
import dl_settings_tests.utils as test_utils


def test_raise_no_value() -> None:
    class Settings(dl_settings.BaseRootSettings):
        field: str = NotImplemented

    with pytest.raises(pydantic.ValidationError):
        Settings()


def test_envs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Settings(dl_settings.BaseRootSettings):
        field: str = NotImplemented

    monkeypatch.setenv("FIELD", "value")

    settings = Settings()
    assert settings.field == "value"


def test_config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_configs: test_utils.TmpConfigs,
) -> None:
    class Settings(dl_settings.BaseRootSettings):
        field: str = NotImplemented

    config = {"field": "value"}
    config_path = tmp_configs.add(config)

    monkeypatch.setenv("CONFIG_PATH", str(config_path))

    settings = Settings()

    assert settings.field == "value"


def test_init() -> None:
    class Settings(dl_settings.BaseRootSettings):
        field: str = NotImplemented

    settings = Settings(field="value")

    assert settings.field == "value"


def test_env_overrides_config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_configs: test_utils.TmpConfigs,
) -> None:
    class Settings(dl_settings.BaseRootSettings):
        field: str = NotImplemented

    config = {"field": "config_value"}
    config_path = tmp_configs.add(config)

    monkeypatch.setenv("FIELD", "env_value")
    monkeypatch.setenv("CONFIG_PATH", str(config_path))

    settings = Settings()

    assert settings.field == "env_value"


def test_multiple_configs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_configs: test_utils.TmpConfigs,
) -> None:
    class Settings(dl_settings.BaseRootSettings):
        field1: str = NotImplemented
        field2: str = NotImplemented

    config1 = {"field1": "config1_value"}
    config2 = {"field2": "config2_value"}

    config1_path = tmp_configs.add(config1)
    config2_path = tmp_configs.add(config2)

    monkeypatch.setenv("CONFIG_PATH", f"{config1_path}, {config2_path}")

    settings = Settings()

    assert settings.field1 == "config1_value"
    assert settings.field2 == "config2_value"


def test_multiple_configs_priority(
    monkeypatch: pytest.MonkeyPatch,
    tmp_configs: test_utils.TmpConfigs,
) -> None:
    class Settings(dl_settings.BaseRootSettings):
        field: str = NotImplemented

    config1 = {"field": "config1_value"}
    config2 = {"field": "config2_value"}

    config1_path = tmp_configs.add(config1)
    config2_path = tmp_configs.add(config2)

    monkeypatch.setenv("CONFIG_PATH", f"{config1_path}, {config2_path}")

    settings = Settings()

    assert settings.field == "config1_value"


def test_nested(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class NestedSettings(dl_settings.BaseSettings):
        field: str = NotImplemented

    class Settings(dl_settings.BaseRootSettings):
        nested: NestedSettings = pydantic.Field(default_factory=NestedSettings)

    monkeypatch.setenv("NESTED__FIELD", "value")

    settings = Settings()
    assert settings.nested.field == "value"


def test_nested_multiple_sources(
    monkeypatch: pytest.MonkeyPatch,
    tmp_configs: test_utils.TmpConfigs,
) -> None:
    class NestedSettings(dl_settings.BaseSettings):
        field1: str = NotImplemented
        field2: str = NotImplemented

    class Settings(dl_settings.BaseRootSettings):
        nested: NestedSettings = pydantic.Field(default_factory=NestedSettings)

    config = {
        "nested": {
            "field1": "config_value",
            "field2": "config_value",
        }
    }
    config_path = tmp_configs.add(config)

    monkeypatch.setenv("NESTED__FIELD1", "env_value")
    monkeypatch.setenv("CONFIG_PATH", str(config_path))

    settings = Settings()

    assert settings.nested.field1 == "env_value"
    assert settings.nested.field2 == "config_value"
