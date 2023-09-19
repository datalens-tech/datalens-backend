from typing import (
    Any,
    Optional,
    Type,
    TypeVar,
)

import attr

from dl_configs.connectors_data import ConnectorsDataBase
from dl_configs.connectors_settings import (
    ConnectorsConfigType,
    ConnectorSettingsBase,
    SettingsFallbackType,
)
from dl_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig


_CONN_DATA_TV = TypeVar("_CONN_DATA_TV", bound=ConnectorsDataBase)


def get_connectors_settings_config(
    full_cgf: ConnectorsConfigType,
    object_like_config_key: str,
    connector_data_class: Type[_CONN_DATA_TV],
) -> Optional[ObjectLikeConfig | _CONN_DATA_TV]:
    if isinstance(full_cgf, connector_data_class):  # installation-style settings
        return full_cgf
    if isinstance(full_cgf, ObjectLikeConfig):  # yaml-style settings
        assert hasattr(full_cgf, "CONNECTORS")
        settings: Optional[ObjectLikeConfig] = getattr(full_cgf.CONNECTORS, object_like_config_key, None)
        if settings is not None:
            for key, setting in settings.items():  # type: str, Any  # converts left ObjectLikeConfigs to dicts
                if isinstance(setting, (tuple, list)):
                    settings._data[key] = [  # noqa  # TODO avoid changing cfg data directly
                        item.to_dict() if isinstance(item, ObjectLikeConfig) else item for item in setting
                    ]
        return settings
    return None  # no settings from a given connector


@attr.s(frozen=True)
class ConnectorSettingsDefinition:
    settings_class: Type[ConnectorSettingsBase] = attr.ib()
    fallback: SettingsFallbackType = attr.ib()
