from typing import Optional

import attr

from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from dl_configs.settings_loaders.meta_definition import s_attrib
from dl_core.connectors.settings.primitives import (
    ConnectorSettingsDefinition,
    get_connectors_settings_config,
)


@attr.s(frozen=True)
class YDBConnectorSettings(ConnectorSettingsBase):
    ENABLE_AUTH_TYPE_PICKER: Optional[bool] = s_attrib("ENABLE_AUTH_TYPE_PICKER", missing=False)  # type: ignore
    DEFAULT_HOST_VALUE: Optional[str] = s_attrib("DEFAULT_HOST_VALUE", missing=None)  # type: ignore
    DEFAULT_SSL_ENABLE_VALUE: bool = s_attrib("DEFAULT_SSL_ENABLE_VALUE", missing=True)  # type: ignore


def ydb_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, ConnectorSettingsBase]:
    cfg = get_connectors_settings_config(full_cfg, object_like_config_key="YDB")
    if cfg is None:
        return {}
    return dict(YDB=YDBConnectorSettings(ENABLE_AUTH_TYPE_PICKER=cfg.ENABLE_AUTH_TYPE_PICKER))  # type: ignore  # 2024-01-30 # TODO: Unexpected keyword argument "ENABLE_AUTH_TYPE_PICKER" for "YDBConnectorSettings"  [call-arg]


class YDBSettingDefinition(ConnectorSettingsDefinition):
    settings_class = YDBConnectorSettings
    fallback = ydb_settings_fallback
