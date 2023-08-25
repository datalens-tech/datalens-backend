from bi_configs.connectors_settings import ConnectorSettingsBase, PostgresConnectorSettings
from bi_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition


def postgresql_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, ConnectorSettingsBase]:
    return dict(POSTGRES=PostgresConnectorSettings())


class PostgreSQLSettingDefinition(ConnectorSettingsDefinition):
    settings_class = PostgresConnectorSettings
    fallback = postgresql_settings_fallback
