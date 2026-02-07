import typing

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
        FIELD: str = NotImplemented

    monkeypatch.setenv("FIELD", "value")

    settings = Settings()
    assert settings.FIELD == "value"


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
        FIELD: str = NotImplemented

    config = {"FIELD": "config_value"}
    config_path = tmp_configs.add(config)

    monkeypatch.setenv("FIELD", "env_value")
    monkeypatch.setenv("CONFIG_PATH", str(config_path))

    settings = Settings()

    assert settings.FIELD == "env_value"


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
        FIELD: str = NotImplemented

    class Settings(dl_settings.BaseRootSettings):
        NESTED: NestedSettings = pydantic.Field(default_factory=NestedSettings)

    monkeypatch.setenv("NESTED__FIELD", "value")

    settings = Settings()
    assert settings.NESTED.FIELD == "value"


def test_nested_multiple_sources(
    monkeypatch: pytest.MonkeyPatch,
    tmp_configs: test_utils.TmpConfigs,
) -> None:
    class NestedSettings(dl_settings.BaseSettings):
        FIELD1: str = NotImplemented
        FIELD2: str = NotImplemented

    class Settings(dl_settings.BaseRootSettings):
        NESTED: NestedSettings = pydantic.Field(default_factory=NestedSettings)

    config = {
        "NESTED": {
            "FIELD1": "config_value",
            "FIELD2": "config_value",
        }
    }
    config_path = tmp_configs.add(config)

    monkeypatch.setenv("NESTED__FIELD1", "env_value")
    monkeypatch.setenv("CONFIG_PATH", str(config_path))

    settings = Settings()

    assert settings.NESTED.FIELD1 == "env_value"
    assert settings.NESTED.FIELD2 == "config_value"


def test_read_top_level_setting_fields_from_nested(
    monkeypatch: pytest.MonkeyPatch,
    tmp_configs: test_utils.TmpConfigs,
) -> None:
    class _RootSettings(dl_settings.BaseRootSettings):
        FIELD3: str = NotImplemented

    class NestedSettings(dl_settings.BaseSettings):
        FIELD1: str = NotImplemented
        FIELD2: str = NotImplemented
        root: _RootSettings = pydantic.Field(default_factory=_RootSettings)

        @property
        def NESTED_FIELD3(self) -> str:
            return self.root.FIELD3

    class Settings(dl_settings.BaseRootSettings):
        NESTED: NestedSettings = pydantic.Field(default_factory=NestedSettings)
        FIELD3: str = "default"

    config = {
        "NESTED": {
            "FIELD1": "config_value",
            "FIELD2": "config_value",
        },
        "FIELD3": "top_level_setting_value",
    }
    config_path = tmp_configs.add(config)

    monkeypatch.setenv("CONFIG_PATH", str(config_path))

    settings = Settings()

    assert settings.NESTED.NESTED_FIELD3 == "top_level_setting_value"


def test_partial_field_aliases_in_child_classes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_configs: test_utils.TmpConfigs,
) -> None:
    class ChildSettingsBase(pydantic.BaseModel):
        FIELD1: str = "default_value"
        FIELD2: str = "default_value"
        FIELD3: str = "default_value"

    class Child1SettingsBase(ChildSettingsBase):
        DEPRECATED_PREFIX: typing.ClassVar[str] = "someprefix_"

        model_config = pydantic.ConfigDict(alias_generator=dl_settings.prefix_alias_generator(DEPRECATED_PREFIX))

    class Child2SettingsBase(ChildSettingsBase):
        DEPRECATED_PREFIX: typing.ClassVar[str] = "otherprefix_"

        model_config = pydantic.ConfigDict(alias_generator=dl_settings.prefix_alias_generator(DEPRECATED_PREFIX))

    class SettingsBase(dl_settings.BaseRootSettings):
        CHILD_1: Child1SettingsBase = pydantic.Field(default_factory=Child1SettingsBase)
        CHILD_2: Child2SettingsBase = pydantic.Field(default_factory=Child2SettingsBase)

    config1 = {
        "CHILD_1": {"someprefix_FIELD1": "value1_1", "FIELD2": "value1_2"},
        "CHILD_2": {"FIELD1": "value2_1", "otherprefix_FIELD2": "value2_2"},
    }

    config_path = tmp_configs.add(config1)

    monkeypatch.setenv("CONFIG_PATH", str(config_path))
    monkeypatch.setenv("CHILD_1__FIELD1", "env_value_1_1")
    monkeypatch.setenv("CHILD_2__FIELD1", "env_value_2_1")
    monkeypatch.setenv("CHILD_2__FIELD3", "env_value_2_3")

    settings = SettingsBase()

    assert settings.CHILD_1.FIELD1 == "env_value_1_1"
    assert settings.CHILD_2.FIELD1 == "env_value_2_1"
    assert settings.CHILD_1.FIELD2 == "value1_2"
    assert settings.CHILD_2.FIELD2 == "value2_2"
    assert settings.CHILD_1.FIELD3 == "default_value"
    assert settings.CHILD_2.FIELD3 == "env_value_2_3"


def test_nested_dict_leaves_case_from_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class NestedSettings(dl_settings.BaseSettings):
        VALUE: str

    class Settings(dl_settings.BaseRootSettings):
        NESTED: dict[str, NestedSettings] = NotImplemented

    with monkeypatch.context() as m:
        m.setenv("NESTED__CHILD__VALUE", "value")
        settings = Settings()
        assert settings.NESTED["CHILD"].VALUE == "value"

    with monkeypatch.context() as m:
        m.setenv("NESTED__child__VALUE", "value")
        settings = Settings()
        assert settings.NESTED["child"].VALUE == "value"
