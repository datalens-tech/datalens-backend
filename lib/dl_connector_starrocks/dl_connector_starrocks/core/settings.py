from typing import ClassVar

from dl_core.connectors.settings.base import ConnectorSettings
from dl_core.connectors.settings.mixins import (
    DatasourceTemplateSettingsMixin,
    TableDatasourceSettingsMixin,
)
from dl_core.connectors.settings.primitives import ConnectorSettingsDefinition

from dl_connector_starrocks.core.constants import CONNECTION_TYPE_STARROCKS


class StarRocksConnectorSettings(ConnectorSettings, TableDatasourceSettingsMixin, DatasourceTemplateSettingsMixin):
    type: str = CONNECTION_TYPE_STARROCKS.value

    root_fallback_env_keys: ClassVar[dict[str, str]] = {
        "CONNECTORS__STARROCKS__ENABLE_DATASOURCE_TEMPLATE": "CONNECTORS_STARROCKS_ENABLE_DATASOURCE_TEMPLATE",
        "CONNECTORS__STARROCKS__ENABLE_TABLE_DATASOURCE_FORM": "CONNECTORS_STARROCKS_ENABLE_TABLE_DATASOURCE_FORM",
    }


class StarRocksSettingDefinition(ConnectorSettingsDefinition):
    pydantic_settings_class = StarRocksConnectorSettings
