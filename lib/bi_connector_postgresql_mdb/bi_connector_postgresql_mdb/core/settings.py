from bi_configs.connectors_settings import ConnectorsConfigType, ConnectorSettingsBase, PostgresConnectorSettings

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition


def postgresql_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    return dict(POSTGRES=PostgresConnectorSettings())


class PostgreSQLMDBSettingDefinition(ConnectorSettingsDefinition):
    settings_class = PostgresConnectorSettings
    fallback = postgresql_settings_fallback
