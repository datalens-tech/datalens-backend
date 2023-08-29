from bi_configs.connectors_settings import ConnectorsConfigType, ConnectorSettingsBase, YDBConnectorSettings

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition


def ydb_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    return dict(YDB=YDBConnectorSettings())


class YDBSettingDefinition(ConnectorSettingsDefinition):
    settings_class = YDBConnectorSettings
    fallback = ydb_settings_fallback
