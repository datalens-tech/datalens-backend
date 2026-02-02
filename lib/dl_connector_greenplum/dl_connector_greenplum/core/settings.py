from typing import ClassVar

from dl_core.connectors.settings.base import ConnectorSettings
from dl_core.connectors.settings.mixins import (
    DatasourceTemplateSettingsMixin,
    TableDatasourceSettingsMixin,
)
from dl_core.connectors.settings.primitives import ConnectorSettingsDefinition

from dl_connector_greenplum.core.constants import CONNECTION_TYPE_GREENPLUM


class GreenplumConnectorSettings(ConnectorSettings, TableDatasourceSettingsMixin, DatasourceTemplateSettingsMixin):
    type: str = CONNECTION_TYPE_GREENPLUM.value

    root_fallback_env_keys: ClassVar[dict[str, str]] = {
        "CONNECTORS__GREENPLUM__ENABLE_DATASOURCE_TEMPLATE": "CONNECTORS_GREENPLUM_ENABLE_DATASOURCE_TEMPLATE",
        "CONNECTORS__GREENPLUM__ENABLE_TABLE_DATASOURCE_FORM": "CONNECTORS_GREENPLUM_ENABLE_TABLE_DATASOURCE_FORM",
    }


class GreenplumSettingDefinition(ConnectorSettingsDefinition):
    pydantic_settings_class = GreenplumConnectorSettings
