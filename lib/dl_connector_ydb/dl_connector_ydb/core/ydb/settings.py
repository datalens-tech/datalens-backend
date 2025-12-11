from typing import Optional

import attr

from dl_configs.connectors_settings import DeprecatedConnectorSettingsBase
from dl_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from dl_configs.settings_loaders.meta_definition import s_attrib
from dl_core.connectors.settings.mixins import DeprecatedDatasourceTemplateSettingsMixin
from dl_core.connectors.settings.primitives import (
    DeprecatedConnectorSettingsDefinition,
    get_connectors_settings_config,
)
from dl_core.connectors.settings.pydantic.base import ConnectorSettings
from dl_core.connectors.settings.pydantic.mixins import DatasourceTemplateSettingsMixin

from dl_connector_ydb.core.ydb.adapter import CONNECTION_TYPE_YDB


@attr.s(frozen=True)
class DeprecatedYDBConnectorSettings(DeprecatedConnectorSettingsBase, DeprecatedDatasourceTemplateSettingsMixin):
    ENABLE_AUTH_TYPE_PICKER: Optional[bool] = s_attrib("ENABLE_AUTH_TYPE_PICKER", missing=False)  # type: ignore
    DEFAULT_HOST_VALUE: Optional[str] = s_attrib("DEFAULT_HOST_VALUE", missing=None)  # type: ignore
    DEFAULT_SSL_ENABLE_VALUE: bool = s_attrib("DEFAULT_SSL_ENABLE_VALUE", missing=True)  # type: ignore


def ydb_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, DeprecatedConnectorSettingsBase]:
    cfg = get_connectors_settings_config(full_cfg, object_like_config_key="YDB")
    if cfg is None:
        settings = DeprecatedYDBConnectorSettings()
    else:
        settings = DeprecatedYDBConnectorSettings(  # type: ignore
            ENABLE_AUTH_TYPE_PICKER=cfg.get("ENABLE_AUTH_TYPE_PICKER", False),
            DEFAULT_HOST_VALUE=cfg.get("DEFAULT_HOST_VALUE", None),
            DEFAULT_SSL_ENABLE_VALUE=cfg.get("DEFAULT_SSL_ENABLE_VALUE", True),
            ENABLE_DATASOURCE_TEMPLATE=cfg.get("ENABLE_DATASOURCE_TEMPLATE", True),
        )
    return dict(YDB=settings)


class DeprecatedYDBSettingDefinition(DeprecatedConnectorSettingsDefinition):
    settings_class = DeprecatedYDBConnectorSettings
    fallback = ydb_settings_fallback


class YDBConnectorSettings(ConnectorSettings, DatasourceTemplateSettingsMixin):
    type: str = CONNECTION_TYPE_YDB.value

    ENABLE_AUTH_TYPE_PICKER: bool | None = False
    DEFAULT_HOST_VALUE: str | None = None
    DEFAULT_SSL_ENABLE_VALUE: bool = True
