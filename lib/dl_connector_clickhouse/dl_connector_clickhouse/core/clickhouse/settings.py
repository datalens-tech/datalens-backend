from typing import ClassVar

from dl_core.connectors.settings.base import ConnectorSettings
from dl_core.connectors.settings.mixins import (
    DatasourceTemplateSettingsMixin,
    TableDatasourceSettingsMixin,
)
from dl_core.connectors.settings.primitives import ConnectorSettingsDefinition

from dl_connector_clickhouse.core.clickhouse_base.constants import CONNECTION_TYPE_CLICKHOUSE


class ClickHouseConnectorSettings(ConnectorSettings, TableDatasourceSettingsMixin, DatasourceTemplateSettingsMixin):
    type: str = CONNECTION_TYPE_CLICKHOUSE.value

    root_fallback_env_keys: ClassVar[dict[str, str]] = {
        "CONNECTORS__CLICKHOUSE__ENABLE_DATASOURCE_TEMPLATE": "CONNECTORS_CLICKHOUSE_ENABLE_DATASOURCE_TEMPLATE",
        "CONNECTORS__CLICKHOUSE__ENABLE_TABLE_DATASOURCE_FORM": "CONNECTORS_CLICKHOUSE_ENABLE_TABLE_DATASOURCE_FORM",
    }


class ClickHouseSettingDefinition(ConnectorSettingsDefinition):
    pydantic_settings_class = ClickHouseConnectorSettings
