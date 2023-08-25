from bi_configs.connectors_settings import ConnectorSettingsBase, YDBConnectorSettings
from bi_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition


def ydb_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, ConnectorSettingsBase]:
    return dict(YDB=YDBConnectorSettings())


class YDBSettingDefinition(ConnectorSettingsDefinition):
    settings_class = YDBConnectorSettings
    fallback = ydb_settings_fallback
