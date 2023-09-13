from bi_configs.connectors_settings import ConnectorsConfigType, ConnectorSettingsBase, ClickHouseConnectorSettings

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition


def clickhouse_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    return dict(CLICKHOUSE=ClickHouseConnectorSettings())


class ClickHouseSettingDefinition(ConnectorSettingsDefinition):
    settings_class = ClickHouseConnectorSettings
    fallback = clickhouse_settings_fallback
