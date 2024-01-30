import attr

from dl_configs.connectors_settings import (
    ConnectorsConfigType,
    ConnectorSettingsBase,
)
from dl_configs.settings_loaders.meta_definition import s_attrib
from dl_core.connectors.settings.primitives import ConnectorSettingsDefinition


@attr.s(frozen=True)
class MetricaConnectorSettings(ConnectorSettingsBase):
    COUNTER_ALLOW_MANUAL_INPUT: bool = s_attrib("COUNTER_ALLOW_MANUAL_INPUT", missing=False)
    ALLOW_AUTO_DASH_CREATION: bool = s_attrib("ALLOW_AUTO_DASH_CREATION", missing=False)


@attr.s(frozen=True)
class AppmetricaConnectorSettings(ConnectorSettingsBase):
    COUNTER_ALLOW_MANUAL_INPUT: bool = s_attrib("COUNTER_ALLOW_MANUAL_INPUT", missing=False)
    ALLOW_AUTO_DASH_CREATION: bool = s_attrib("ALLOW_AUTO_DASH_CREATION", missing=False)


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
