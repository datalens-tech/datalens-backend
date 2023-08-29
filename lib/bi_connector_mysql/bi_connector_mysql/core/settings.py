from bi_configs.connectors_settings import ConnectorsConfigType, ConnectorSettingsBase, MysqlConnectorSettings

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition


def mysql_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    return dict(MYSQL=MysqlConnectorSettings())


class MySQLSettingDefinition(ConnectorSettingsDefinition):
    settings_class = MysqlConnectorSettings
    fallback = mysql_settings_fallback
