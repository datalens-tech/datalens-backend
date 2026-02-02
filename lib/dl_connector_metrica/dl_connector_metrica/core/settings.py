from typing import ClassVar

from dl_core.connectors.settings.base import ConnectorSettings
from dl_core.connectors.settings.primitives import ConnectorSettingsDefinition

from dl_connector_metrica.core.constants import (
    CONNECTION_TYPE_APPMETRICA_API,
    CONNECTION_TYPE_METRICA_API,
)


class MetricaConnectorSettings(ConnectorSettings):
    type: str = CONNECTION_TYPE_METRICA_API.value

    COUNTER_ALLOW_MANUAL_INPUT: bool = False
    ALLOW_AUTO_DASH_CREATION: bool = False
    BACKEND_DRIVEN_FORM: bool = False

    root_fallback_env_keys: ClassVar[dict[str, str]] = {
        "CONNECTORS__METRIKA_API__COUNTER_ALLOW_MANUAL_INPUT": "CONNECTORS_METRIKA_API_COUNTER_ALLOW_MANUAL_INPUT",
        "CONNECTORS__METRIKA_API__ALLOW_AUTO_DASH_CREATION": "CONNECTORS_METRIKA_API_ALLOW_AUTO_DASH_CREATION",
        "CONNECTORS__METRIKA_API__BACKEND_DRIVEN_FORM": "CONNECTORS_METRIKA_API_BACKEND_DRIVEN_FORM",
    }


class MetricaSettingDefinition(ConnectorSettingsDefinition):
    pydantic_settings_class = MetricaConnectorSettings


class AppmetricaConnectorSettings(ConnectorSettings):
    type: str = CONNECTION_TYPE_APPMETRICA_API.value

    COUNTER_ALLOW_MANUAL_INPUT: bool = False
    ALLOW_AUTO_DASH_CREATION: bool = False
    BACKEND_DRIVEN_FORM: bool = False

    root_fallback_env_keys: ClassVar[dict[str, str]] = {
        "CONNECTORS__APPMETRICA_API__COUNTER_ALLOW_MANUAL_INPUT": "CONNECTORS_APPMETRICA_API_COUNTER_ALLOW_MANUAL_INPUT",
        "CONNECTORS__APPMETRICA_API__ALLOW_AUTO_DASH_CREATION": "CONNECTORS_APPMETRICA_API_ALLOW_AUTO_DASH_CREATION",
        "CONNECTORS__APPMETRICA_API__BACKEND_DRIVEN_FORM": "CONNECTORS_APPMETRICA_API_BACKEND_DRIVEN_FORM",
    }


class AppMetricaSettingDefinition(ConnectorSettingsDefinition):
    pydantic_settings_class = AppmetricaConnectorSettings
