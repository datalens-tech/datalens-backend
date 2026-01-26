from typing import ClassVar

from dl_core.connectors.settings.base import ConnectorSettings
from dl_core.connectors.settings.mixins import (
    DatasourceTemplateSettingsMixin,
    TableDatasourceSettingsMixin,
)
from dl_core.connectors.settings.primitives import ConnectorSettingsDefinition

from dl_connector_oracle.core.constants import CONNECTION_TYPE_ORACLE


class OracleConnectorSettings(ConnectorSettings, TableDatasourceSettingsMixin, DatasourceTemplateSettingsMixin):
    type: str = CONNECTION_TYPE_ORACLE.value

    root_fallback_env_keys: ClassVar[dict[str, str]] = {
        "CONNECTORS__ORACLE__ENABLE_DATASOURCE_TEMPLATE": "CONNECTORS_ORACLE_ENABLE_DATASOURCE_TEMPLATE",
        "CONNECTORS__ORACLE__ENABLE_TABLE_DATASOURCE_FORM": "CONNECTORS_ORACLE_ENABLE_TABLE_DATASOURCE_FORM",
    }


class OracleSettingDefinition(ConnectorSettingsDefinition):
    pydantic_settings_class = OracleConnectorSettings
