from typing import ClassVar

import pydantic

from dl_core.connectors.settings.base import ConnectorSettings
from dl_core.connectors.settings.mixins import (
    DatasourceTemplateSettingsMixin,
    DirectSQLSettingsMixin,
    QuerySettingsSettingsMixin,
    TableDatasourceSettingsMixin,
)
from dl_core.connectors.settings.primitives import ConnectorSettingsDefinition
from dl_core.connectors.settings.query_settings import QuerySettingsSettings

from dl_connector_clickhouse.core.clickhouse_base.constants import CONNECTION_TYPE_CLICKHOUSE


class ClickHouseQuerySettingsSettings(QuerySettingsSettings):
    """ClickHouse defaults for per-dataset query settings.

    `ALLOWED=None` lets every CH session setting through (subject to `FORBIDDEN`).
    `FORBIDDEN` covers the names DataLens itself manages as HTTP request params
    plus reserved adapter-owned HTTP params like `database`.
    """

    ALLOWED: frozenset[str] | None = None
    FORBIDDEN: frozenset[str] = frozenset(
        {
            "readonly",
            "join_use_nulls",
            "send_progress_in_http_headers",
            "output_format_json_quote_denormals",
            "database",
        }
    )


class ClickHouseConnectorSettings(
    ConnectorSettings,
    TableDatasourceSettingsMixin,
    DatasourceTemplateSettingsMixin,
    QuerySettingsSettingsMixin,
    DirectSQLSettingsMixin,
):
    type: str = CONNECTION_TYPE_CLICKHOUSE.value

    root_fallback_env_keys: ClassVar[dict[str, str]] = {
        "CONNECTORS__CLICKHOUSE__ENABLE_DATASOURCE_TEMPLATE": "CONNECTORS_CLICKHOUSE_ENABLE_DATASOURCE_TEMPLATE",
        "CONNECTORS__CLICKHOUSE__ENABLE_TABLE_DATASOURCE_FORM": "CONNECTORS_CLICKHOUSE_ENABLE_TABLE_DATASOURCE_FORM",
        "CONNECTORS__CLICKHOUSE__ALLOW_EXPERIMENTAL_FEATURES": "CONNECTORS_CLICKHOUSE_ALLOW_EXPERIMENTAL_FEATURES",
    }

    ALLOW_EXPERIMENTAL_FEATURES: bool = False
    ALLOW_SSL_CA_VERIFY_OPTION: bool = False
    QUERY_SETTINGS: ClickHouseQuerySettingsSettings = pydantic.Field(default_factory=ClickHouseQuerySettingsSettings)


class ClickHouseSettingDefinition(ConnectorSettingsDefinition):
    pydantic_settings_class = ClickHouseConnectorSettings
