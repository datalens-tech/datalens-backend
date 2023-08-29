from bi_configs.connectors_settings import (
    ConnectorsConfigType, ConnectorSettingsBase, MetricaConnectorSettings, AppmetricaConnectorSettings,
)

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition


def metrica_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    return dict(METRIKA_API=MetricaConnectorSettings())


class MetricaSettingDefinition(ConnectorSettingsDefinition):
    settings_class = MetricaConnectorSettings
    fallback = metrica_settings_fallback


def appmetrica_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    return dict(APPMETRICA_API=AppmetricaConnectorSettings())


class AppMetricaSettingDefinition(ConnectorSettingsDefinition):
    settings_class = AppmetricaConnectorSettings
    fallback = appmetrica_settings_fallback
