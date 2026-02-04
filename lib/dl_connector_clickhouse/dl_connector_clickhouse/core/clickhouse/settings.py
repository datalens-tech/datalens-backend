from typing import ClassVar

import attr

from dl_configs.connectors_settings import DeprecatedConnectorSettingsBase
from dl_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from dl_configs.settings_loaders.meta_definition import s_attrib
from dl_core.connectors.settings.base import ConnectorSettings
from dl_core.connectors.settings.mixins import (
    DatasourceTemplateSettingsMixin,
    DeprecatedDatasourceTemplateSettingsMixin,
    DeprecatedTableDatasourceSettingsMixin,
    TableDatasourceSettingsMixin,
)
from dl_core.connectors.settings.primitives import (
    ConnectorSettingsDefinition,
    get_connectors_settings_config,
)

from dl_connector_clickhouse.core.clickhouse_base.constants import CONNECTION_TYPE_CLICKHOUSE


@attr.s(frozen=True)
class DeprecatedClickHouseConnectorSettings(
    DeprecatedConnectorSettingsBase,
    DeprecatedDatasourceTemplateSettingsMixin,
    DeprecatedTableDatasourceSettingsMixin,
):
    ALLOW_EXPERIMENTAL_FEATURES: bool = s_attrib("ENABLE_DATASOURCE_TEMPLATE", missing=True)  # type: ignore


def clickhouse_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, DeprecatedConnectorSettingsBase]:
    cfg = get_connectors_settings_config(full_cfg, object_like_config_key="CLICKHOUSE")
    if cfg is None:
        settings = DeprecatedClickHouseConnectorSettings()
    else:
        settings = DeprecatedClickHouseConnectorSettings(  # type: ignore
            ENABLE_DATASOURCE_TEMPLATE=cfg.get("ENABLE_DATASOURCE_TEMPLATE", True),
            ENABLE_TABLE_DATASOURCE_FORM=cfg.get("ENABLE_TABLE_DATASOURCE_FORM", True),
        )

    return dict(CLICKHOUSE=settings)


class ClickHouseConnectorSettings(ConnectorSettings, TableDatasourceSettingsMixin, DatasourceTemplateSettingsMixin):
    type: str = CONNECTION_TYPE_CLICKHOUSE.value

    root_fallback_env_keys: ClassVar[dict[str, str]] = {
        "CONNECTORS__CLICKHOUSE__ENABLE_DATASOURCE_TEMPLATE": "CONNECTORS_CLICKHOUSE_ENABLE_DATASOURCE_TEMPLATE",
        "CONNECTORS__CLICKHOUSE__ENABLE_TABLE_DATASOURCE_FORM": "CONNECTORS_CLICKHOUSE_ENABLE_TABLE_DATASOURCE_FORM",
        "CONNECTORS__CLICKHOUSE__ALLOW_EXPERIMENTAL_FEATURES": "CONNECTORS_CLICKHOUSE_ALLOW_EXPERIMENTAL_FEATURES",
    }

    ALLOW_EXPERIMENTAL_FEATURES: bool = False


class ClickHouseSettingDefinition(ConnectorSettingsDefinition):
    settings_class = DeprecatedClickHouseConnectorSettings
    fallback = clickhouse_settings_fallback

    pydantic_settings_class = ClickHouseConnectorSettings
