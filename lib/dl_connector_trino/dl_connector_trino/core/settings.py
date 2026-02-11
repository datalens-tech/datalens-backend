from typing import ClassVar

from dl_core.connectors.settings.base import ConnectorSettings
from dl_core.connectors.settings.mixins import TableDatasourceSettingsMixin
from dl_core.connectors.settings.primitives import ConnectorSettingsDefinition

from dl_connector_trino.core.constants import CONNECTION_TYPE_TRINO


class TrinoConnectorSettings(ConnectorSettings, TableDatasourceSettingsMixin):
    type: str = CONNECTION_TYPE_TRINO.value

    root_fallback_env_keys: ClassVar[dict[str, str]] = {
        "CONNECTORS__TRINO__ENABLE_TABLE_DATASOURCE_FORM": "CONNECTORS_TRINO_ENABLE_TABLE_DATASOURCE_FORM",
    }


class TrinoSettingDefinition(ConnectorSettingsDefinition):
    pydantic_settings_class = TrinoConnectorSettings
