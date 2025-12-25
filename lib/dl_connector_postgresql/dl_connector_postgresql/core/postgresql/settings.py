from typing import ClassVar

import attr

from dl_configs.connectors_settings import DeprecatedConnectorSettingsBase
from dl_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
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

from dl_connector_postgresql.core.postgresql.constants import CONNECTION_TYPE_POSTGRES


@attr.s(frozen=True)
class DeprecatedPostgreSQLConnectorSettings(
    DeprecatedConnectorSettingsBase,
    DeprecatedDatasourceTemplateSettingsMixin,
    DeprecatedTableDatasourceSettingsMixin,
):
    pass


def postgresql_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, DeprecatedConnectorSettingsBase]:
    cfg = get_connectors_settings_config(full_cfg, object_like_config_key="POSTGRES")
    if cfg is None:
        settings = DeprecatedPostgreSQLConnectorSettings()
    else:
        settings = DeprecatedPostgreSQLConnectorSettings(  # type: ignore
            ENABLE_DATASOURCE_TEMPLATE=cfg.get("ENABLE_DATASOURCE_TEMPLATE", True),
            ENABLE_TABLE_DATASOURCE_FORM=cfg.get("ENABLE_TABLE_DATASOURCE_FORM", True),
        )
    return dict(POSTGRES=settings)


class PostgreSQLConnectorSettings(ConnectorSettings, TableDatasourceSettingsMixin, DatasourceTemplateSettingsMixin):
    type: str = CONNECTION_TYPE_POSTGRES.value

    pydantic_env_fallback: ClassVar[dict[str, str]] = {
        "CONNECTORS__POSTGRES__ENABLE_DATASOURCE_TEMPLATE": "CONNECTORS_POSTGRES_ENABLE_DATASOURCE_TEMPLATE",
        "CONNECTORS__POSTGRES__ENABLE_TABLE_DATASOURCE_FORM": "CONNECTORS_POSTGRES_ENABLE_TABLE_DATASOURCE_FORM",
    }


class PostgreSQLSettingDefinition(ConnectorSettingsDefinition):
    settings_class = DeprecatedPostgreSQLConnectorSettings
    fallback = postgresql_settings_fallback

    pydantic_settings_class = PostgreSQLConnectorSettings
