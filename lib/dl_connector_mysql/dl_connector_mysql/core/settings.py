from typing import ClassVar

from dl_core.connectors.settings.base import ConnectorSettings
from dl_core.connectors.settings.mixins import (
    DatasourceTemplateSettingsMixin,
    TableDatasourceSettingsMixin,
)
from dl_core.connectors.settings.primitives import ConnectorSettingsDefinition

from dl_connector_mysql.core.constants import CONNECTION_TYPE_MYSQL


class MySQLConnectorSettings(ConnectorSettings, TableDatasourceSettingsMixin, DatasourceTemplateSettingsMixin):
    type: str = CONNECTION_TYPE_MYSQL.value

    root_fallback_env_keys: ClassVar[dict[str, str]] = {
        "CONNECTORS__MYSQL__ENABLE_DATASOURCE_TEMPLATE": "CONNECTORS_MYSQL_ENABLE_DATASOURCE_TEMPLATE",
        "CONNECTORS__MYSQL__ENABLE_TABLE_DATASOURCE_FORM": "CONNECTORS_MYSQL_ENABLE_TABLE_DATASOURCE_FORM",
    }


class MySQLSettingDefinition(ConnectorSettingsDefinition):
    pydantic_settings_class = MySQLConnectorSettings
