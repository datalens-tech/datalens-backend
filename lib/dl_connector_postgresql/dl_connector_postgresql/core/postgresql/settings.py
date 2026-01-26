from typing import ClassVar

from dl_core.connectors.settings.base import ConnectorSettings
from dl_core.connectors.settings.mixins import (
    DatasourceTemplateSettingsMixin,
    TableDatasourceSettingsMixin,
)
from dl_core.connectors.settings.primitives import ConnectorSettingsDefinition

from dl_connector_postgresql.core.postgresql.constants import CONNECTION_TYPE_POSTGRES


class PostgreSQLConnectorSettings(ConnectorSettings, TableDatasourceSettingsMixin, DatasourceTemplateSettingsMixin):
    type: str = CONNECTION_TYPE_POSTGRES.value

    root_fallback_env_keys: ClassVar[dict[str, str]] = {
        "CONNECTORS__POSTGRES__ENABLE_DATASOURCE_TEMPLATE": "CONNECTORS_POSTGRES_ENABLE_DATASOURCE_TEMPLATE",
        "CONNECTORS__POSTGRES__ENABLE_TABLE_DATASOURCE_FORM": "CONNECTORS_POSTGRES_ENABLE_TABLE_DATASOURCE_FORM",
    }


class PostgreSQLSettingDefinition(ConnectorSettingsDefinition):
    pydantic_settings_class = PostgreSQLConnectorSettings
