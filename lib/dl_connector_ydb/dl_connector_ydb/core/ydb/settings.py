from typing import ClassVar

from dl_core.connectors.settings.base import ConnectorSettings
from dl_core.connectors.settings.mixins import DatasourceTemplateSettingsMixin
from dl_core.connectors.settings.primitives import ConnectorSettingsDefinition

from dl_connector_ydb.core.ydb.adapter import CONNECTION_TYPE_YDB


class YDBConnectorSettings(ConnectorSettings, DatasourceTemplateSettingsMixin):
    type: str = CONNECTION_TYPE_YDB.value

    ENABLE_AUTH_TYPE_PICKER: bool | None = False
    DEFAULT_HOST_VALUE: str | None = None
    DEFAULT_SSL_ENABLE_VALUE: bool = True

    root_fallback_env_keys: ClassVar[dict[str, str]] = {
        "CONNECTORS__YDB__ENABLE_AUTH_TYPE_PICKER": "CONNECTORS_YDB_ENABLE_AUTH_TYPE_PICKER",
        "CONNECTORS__YDB__DEFAULT_HOST_VALUE": "CONNECTORS_YDB_DEFAULT_HOST_VALUE",
        "CONNECTORS__YDB__DEFAULT_SSL_ENABLE_VALUE": "CONNECTORS_YDB_DEFAULT_SSL_ENABLE_VALUE",
        "CONNECTORS__YDB__ENABLE_DATASOURCE_TEMPLATE": "CONNECTORS_YDB_ENABLE_DATASOURCE_TEMPLATE",
    }


class YDBSettingDefinition(ConnectorSettingsDefinition):
    pydantic_settings_class = YDBConnectorSettings
