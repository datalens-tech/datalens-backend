# dl_settings

This package contains all the basic tooling needed to migrate from dl_configs that uses attr package to pydantic_settings.

## Usage

Idea is to provide a simple way to migrate from dl_configs to pydantic_settings in gradual way. For this purpose several fallbacks are provided.

For example assume old Settings class exists:

```python
@attr.s(frozen=True)
class AppSettings:
    VAR1: bool = s_attrib("DL_VAR1", missing=False)


def app_factory():
    settings = load_settings_from_env_with_fallback(AppSettings)
    ...
```

We can introduce new pydantic settings class to use without moving all the settings at once.

```python
import dl_settings


@attr.s(frozen=True)
class DeprecatedAppSettings:
    VAR1: bool = s_attrib("DL_VAR1", missing=False)


class AppSettings(
    dl_settings.WithFallbackGetAttr,
    dl_settings.BaseRootSettings,
):
    ...


def app_factory():
    deprecated_settings = load_settings_from_env_with_fallback(DeprecatedAppSettings)
    settings = AppSettings(fallback=deprecated_settings)
```

After all old settings are moved we can remove the old class and fallback mixin.

```python
import pydantic

import dl_settings


class AppSettings(
    dl_settings.BaseRootSettings,
):
    VAR1: bool = pydantic.Field(default=False, alias="DL_VAR1")


def app_factory():
    settings = AppSettings()
```

New style of settings uses double underscore to separate nested settings, which can break backward compatibility. To fix this we can use `dl_settings.WithFallbackEnvSource` class. NEVER USE FOR NEW SETTINGS, ONLY FOR MIGRATION.

Old settings:

```python
class NestedSettings(SettingsBase):
    var1: bool = s_attrib("VAR1", missing=False)

    @classmethod
    def from_json(cls, json_value: typing.Any) -> typing_extensions.Self:
        assert isinstance(json_value, typing.Mapping)
        return cls(
            var1=json_value.get("VAR1", False),
        )


@attr.s(frozen=True)
class AppSettings:
    NESTED: NestedSettings = s_attrib(
      "NESTED",
      fallback_factory=lambda cfg: NestedSettings.from_json(cfg.get("NESTED")),
    )
```

New settings:

```python
class NestedSettings(dl_settings.BaseSettings):
    var1: bool = False


class AppSettings(
    dl_settings.WithFallbackEnvSource,
    dl_settings.BaseRootSettings,
):
    nested: NestedSettings = NestedSettings()

    fallback_env_keys = {
        "NESTED__VAR1": "NESTED_VAR1",
    }
```

Eventually we should remove old settings names and fallbacks (while using fallback settings warning is spammed to console).

```python
class NestedSettings(dl_settings.BaseSettings):
    var1: bool = False


class AppSettings(
    dl_settings.BaseRootSettings,
):
    nested: NestedSettings = NestedSettings()
```
