from typing import Optional

import attr

from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from dl_configs.settings_loaders.meta_definition import s_attrib
from dl_core.connectors.settings.mixins import DatasourceTemplateSettingsMixin
from dl_core.connectors.settings.primitives import (
    ConnectorSettingsDefinition,
    get_connectors_settings_config,
)


@attr.s(frozen=True)
class YDBConnectorSettings(ConnectorSettingsBase, DatasourceTemplateSettingsMixin):
    ENABLE_AUTH_TYPE_PICKER: Optional[bool] = s_attrib("ENABLE_AUTH_TYPE_PICKER", missing=False)  # type: ignore
    DEFAULT_HOST_VALUE: Optional[str] = s_attrib("DEFAULT_HOST_VALUE", missing=None)  # type: ignore
    DEFAULT_SSL_ENABLE_VALUE: bool = s_attrib("DEFAULT_SSL_ENABLE_VALUE", missing=True)  # type: ignore


def ydb_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, ConnectorSettingsBase]:
    cfg = get_connectors_settings_config(full_cfg, object_like_config_key="YDB")
    if cfg is None:
        settings = YDBConnectorSettings()
    else:
        settings = YDBConnectorSettings(  # type: ignore
            ENABLE_AUTH_TYPE_PICKER=cfg.get("ENABLE_AUTH_TYPE_PICKER", False),
            DEFAULT_HOST_VALUE=cfg.get("DEFAULT_HOST_VALUE", None),
            DEFAULT_SSL_ENABLE_VALUE=cfg.get("DEFAULT_SSL_ENABLE_VALUE", True),
            ENABLE_DATASOURCE_TEMPLATE=cfg.get("ENABLE_DATASOURCE_TEMPLATE", True),
        )
    return dict(YDB=settings)


class YDBSettingDefinition(ConnectorSettingsDefinition):
    settings_class = YDBConnectorSettings
    fallback = ydb_settings_fallback
