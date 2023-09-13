from bi_configs.connectors_settings import ConnectorsConfigType, ConnectorSettingsBase, GreenplumConnectorSettings

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition


def greenplum_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    return dict(GREENPLUM=GreenplumConnectorSettings())


class GreenplumMDBSettingDefinition(ConnectorSettingsDefinition):
    settings_class = GreenplumConnectorSettings
    fallback = greenplum_settings_fallback
