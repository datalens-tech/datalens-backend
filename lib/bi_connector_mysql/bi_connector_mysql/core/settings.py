from bi_configs.connectors_settings import ConnectorSettingsBase, MysqlConnectorSettings
from bi_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition


def mysql_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, ConnectorSettingsBase]:
    return dict(MYSQL=MysqlConnectorSettings())


class MySQLSettingDefinition(ConnectorSettingsDefinition):
    settings_class = MysqlConnectorSettings
    fallback = mysql_settings_fallback
