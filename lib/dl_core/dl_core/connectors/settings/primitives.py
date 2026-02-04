from typing import (  # noqa: F401
    Any,
    ClassVar,
    Optional,
    Type,
)

import attr

from dl_configs.connectors_settings import (
    DeprecatedConnectorSettingsBase,
    SettingsFallbackType,
)
from dl_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from dl_core.connectors.settings.base import ConnectorSettings


def get_connectors_settings_config(
    full_cgf: ObjectLikeConfig,
    object_like_config_key: str,
) -> ObjectLikeConfig | None:
    assert hasattr(full_cgf, "CONNECTORS")
    settings: ObjectLikeConfig | None = getattr(full_cgf.CONNECTORS, object_like_config_key, None)
    if settings is not None:
        for key, setting in settings.items():  # type: str, Any  # converts left ObjectLikeConfigs to dicts
            if isinstance(setting, (tuple, list)):
                settings._data[key] = [  # noqa  # TODO avoid changing cfg data directly
                    item.to_dict() if isinstance(item, ObjectLikeConfig) else item for item in setting
                ]
    return settings


@attr.s(frozen=True)
class ConnectorSettingsDefinition:
    settings_class: type[DeprecatedConnectorSettingsBase] = attr.ib()
    fallback: SettingsFallbackType = attr.ib()

    pydantic_settings_class: ClassVar[type[ConnectorSettings]] = ConnectorSettings
