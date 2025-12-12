import attr

from dl_configs.connectors_settings import DeprecatedConnectorSettingsBase
from dl_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from dl_configs.settings_loaders.meta_definition import s_attrib
from dl_core.connectors.settings.primitives import (
    DeprecatedConnectorSettingsDefinition,
    get_connectors_settings_config,
)
from dl_core.connectors.settings.pydantic.base import ConnectorSettings

from dl_connector_metrica.core.constants import (
    CONNECTION_TYPE_APPMETRICA_API,
    CONNECTION_TYPE_METRICA_API,
)


@attr.s(frozen=True)
class DeprecatedMetricaConnectorSettings(DeprecatedConnectorSettingsBase):
    COUNTER_ALLOW_MANUAL_INPUT: bool = s_attrib("COUNTER_ALLOW_MANUAL_INPUT", missing=False)  # type: ignore
    ALLOW_AUTO_DASH_CREATION: bool = s_attrib("ALLOW_AUTO_DASH_CREATION", missing=False)  # type: ignore
    BACKEND_DRIVEN_FORM: bool = s_attrib("BACKEND_DRIVEN_FORM", missing=False)  # type: ignore


@attr.s(frozen=True)
class DeprecatedAppmetricaConnectorSettings(DeprecatedConnectorSettingsBase):
    COUNTER_ALLOW_MANUAL_INPUT: bool = s_attrib("COUNTER_ALLOW_MANUAL_INPUT", missing=False)  # type: ignore
    ALLOW_AUTO_DASH_CREATION: bool = s_attrib("ALLOW_AUTO_DASH_CREATION", missing=False)  # type: ignore
    BACKEND_DRIVEN_FORM: bool = s_attrib("BACKEND_DRIVEN_FORM", missing=False)  # type: ignore


def metrica_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, DeprecatedConnectorSettingsBase]:
    cfg = get_connectors_settings_config(full_cfg, object_like_config_key="METRIKA_API")
    if cfg is None:
        return {}
    return dict(METRIKA_API=DeprecatedMetricaConnectorSettings(BACKEND_DRIVEN_FORM=cfg.BACKEND_DRIVEN_FORM))  # type: ignore  # 2024-09-18 # TODO: Unexpected keyword argument "BACKEND_DRIVEN_FORM" for "AppmetricaConnectorSettings"  [call-arg]


class DeprecatedMetricaSettingDefinition(DeprecatedConnectorSettingsDefinition):
    settings_class = DeprecatedMetricaConnectorSettings
    fallback = metrica_settings_fallback


def appmetrica_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, DeprecatedConnectorSettingsBase]:
    cfg = get_connectors_settings_config(full_cfg, object_like_config_key="APPMETRICA_API")
    if cfg is None:
        return {}
    return dict(APPMETRICA_API=DeprecatedAppmetricaConnectorSettings(BACKEND_DRIVEN_FORM=cfg.BACKEND_DRIVEN_FORM))  # type: ignore  # 2024-09-18 # TODO: Unexpected keyword argument "BACKEND_DRIVEN_FORM" for "AppmetricaConnectorSettings"  [call-arg]


class DeprecatedAppMetricaSettingDefinition(DeprecatedConnectorSettingsDefinition):
    settings_class = DeprecatedAppmetricaConnectorSettings
    fallback = appmetrica_settings_fallback


class MetricaConnectorSettings(ConnectorSettings):
    type: str = CONNECTION_TYPE_METRICA_API.value

    COUNTER_ALLOW_MANUAL_INPUT: bool = False
    ALLOW_AUTO_DASH_CREATION: bool = False
    BACKEND_DRIVEN_FORM: bool = False


class AppmetricaConnectorSettings(ConnectorSettings):
    type: str = CONNECTION_TYPE_APPMETRICA_API.value

    COUNTER_ALLOW_MANUAL_INPUT: bool = False
    ALLOW_AUTO_DASH_CREATION: bool = False
    BACKEND_DRIVEN_FORM: bool = False
