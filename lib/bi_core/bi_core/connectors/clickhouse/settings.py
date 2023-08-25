from bi_configs.connectors_settings import ConnectorSettingsBase, ClickHouseConnectorSettings
from bi_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition


def clickhouse_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, ConnectorSettingsBase]:
    return dict(CLICKHOUSE=ClickHouseConnectorSettings())


class ClickHouseSettingDefinition(ConnectorSettingsDefinition):
    settings_class = ClickHouseConnectorSettings
    fallback = clickhouse_settings_fallback
