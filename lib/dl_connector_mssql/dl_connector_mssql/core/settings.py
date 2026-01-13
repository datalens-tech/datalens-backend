from typing import ClassVar

from dl_core.connectors.settings.base import ConnectorSettings
from dl_core.connectors.settings.mixins import (
    DatasourceTemplateSettingsMixin,
    TableDatasourceSettingsMixin,
)
from dl_core.connectors.settings.primitives import ConnectorSettingsDefinition

from dl_connector_mssql.core.constants import CONNECTION_TYPE_MSSQL


class MSSQLConnectorSettings(ConnectorSettings, TableDatasourceSettingsMixin, DatasourceTemplateSettingsMixin):
    type: str = CONNECTION_TYPE_MSSQL.value

    root_fallback_env_keys: ClassVar[dict[str, str]] = {
        "CONNECTORS__MSSQL__ENABLE_DATASOURCE_TEMPLATE": "CONNECTORS_MSSQL_ENABLE_DATASOURCE_TEMPLATE",
        "CONNECTORS__MSSQL__ENABLE_TABLE_DATASOURCE_FORM": "CONNECTORS_MSSQL_ENABLE_TABLE_DATASOURCE_FORM",
    }


class MSSQLSettingDefinition(ConnectorSettingsDefinition):
    pydantic_settings_class = MSSQLConnectorSettings
