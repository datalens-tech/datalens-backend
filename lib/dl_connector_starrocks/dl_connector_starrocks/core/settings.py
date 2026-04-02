from dl_core.connectors.settings.base import ConnectorSettings
from dl_core.connectors.settings.mixins import (
    DatasourceTemplateSettingsMixin,
    TableDatasourceSettingsMixin,
)
from dl_core.connectors.settings.primitives import ConnectorSettingsDefinition

from dl_connector_starrocks.core.constants import CONNECTION_TYPE_STARROCKS


class StarRocksConnectorSettings(ConnectorSettings, TableDatasourceSettingsMixin, DatasourceTemplateSettingsMixin):
    type: str = CONNECTION_TYPE_STARROCKS.value


class StarRocksSettingDefinition(ConnectorSettingsDefinition):
    pydantic_settings_class = StarRocksConnectorSettings
