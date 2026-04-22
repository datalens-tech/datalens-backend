# dl_settings

This package contains all the basic tooling needed to migrate from dl_configs that uses attr package to pydantic_settings.

## Conventions

### Field naming: `UPPER_SNAKE_CASE`

All subclasses of `dl_settings.BaseSettings` (and its derivatives — `BaseRootSettings`, `TypedBaseSettings`, mixins, etc.) declare their fields in **UPPER_SNAKE_CASE**:

```python
class NestedSettings(dl_settings.BaseSettings):
    ENABLED: bool = False
    ITEMS: list[str] | None = None


class AppSettings(dl_settings.BaseRootSettings):
    NESTED: NestedSettings = pydantic.Field(default_factory=NestedSettings)
    VAR1: bool = pydantic.Field(default=False, alias="DL_VAR1")
```

Construct and access them with the same casing — `NestedSettings(ENABLED=True, ITEMS=[...])`, `settings.NESTED.ENABLED`. This matches the rest of the repo (`ConnectorSettings`, mixins, `ClickHouseConnectorSettings`, etc.) and the env-var conventions (env names are upper-case).

### Overriding defaults: subclass, don't use `lambda`

When a `dl_settings.BaseSettings` descendant needs different defaults than its parent, declare a subclass that overrides the defaults and use the subclass as the field type. Do **not** use `default_factory=lambda: Parent(FIELD=...)` or similar inline factories.

Wrong:

```python
class AppSettings(dl_settings.BaseRootSettings):
    NESTED: NestedSettings = pydantic.Field(
        default_factory=lambda: NestedSettings(ENABLED=True),
    )
```

Right:

```python
class CustomNestedSettings(NestedSettings):
    ENABLED: bool = True


class AppSettings(dl_settings.BaseRootSettings):
    NESTED: CustomNestedSettings = pydantic.Field(default_factory=CustomNestedSettings)
```

Subclassing keeps the type accurate (so env-var resolution, validation, and downstream type checks all see the real shape), makes the override discoverable, and composes cleanly with further subclassing. Lambdas hide the shape behind an opaque factory.

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
    VAR1: bool = False


class AppSettings(
    dl_settings.WithFallbackEnvSource,
    dl_settings.BaseRootSettings,
):
    NESTED: NestedSettings = NestedSettings()

    fallback_env_keys = {
        "NESTED__VAR1": "NESTED_VAR1",
    }
```

Eventually we should remove old settings names and fallbacks (while using fallback settings warning is spammed to console).

```python
class NestedSettings(dl_settings.BaseSettings):
    VAR1: bool = False


class AppSettings(
    dl_settings.BaseRootSettings,
):
    NESTED: NestedSettings = NestedSettings()
```
