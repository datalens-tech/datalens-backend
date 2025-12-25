from dl_configs.connectors_settings import (
    DeprecatedConnectorSettingsBase,
    SettingsFallbackType,
)
from dl_constants.enums import ConnectionType
from dl_core.connectors.settings.base import ConnectorSettings


CONNECTORS_SETTINGS_CLASSES: dict[ConnectionType, type[DeprecatedConnectorSettingsBase]] = {}
CONNECTORS_SETTINGS_FALLBACKS: dict[ConnectionType, SettingsFallbackType] = {}


def register_connector_settings_class(
    conn_type: ConnectionType,
    settings_class: type[DeprecatedConnectorSettingsBase],
    fallback: SettingsFallbackType,
    pydantic_settings_class: type[ConnectorSettings],
) -> None:
    if (registered_settings_class := CONNECTORS_SETTINGS_CLASSES.get(conn_type)) is not None:
        assert registered_settings_class == settings_class, f"{registered_settings_class} vs {settings_class}"
        assert CONNECTORS_SETTINGS_FALLBACKS[conn_type] == fallback
    else:
        assert conn_type not in CONNECTORS_SETTINGS_FALLBACKS
        CONNECTORS_SETTINGS_CLASSES[conn_type] = settings_class
        CONNECTORS_SETTINGS_FALLBACKS[conn_type] = fallback
    # delete  above after moving to pydantic settings

    ConnectorSettings.register(conn_type.value, pydantic_settings_class)
