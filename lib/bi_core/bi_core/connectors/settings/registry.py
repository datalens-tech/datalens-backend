from typing import Type

from bi_configs.connectors_settings import ConnectorSettingsBase
from bi_constants.enums import ConnectionType


CONNECTORS_SETTINGS_CLASSES: dict[ConnectionType, Type[ConnectorSettingsBase]] = {}


def register_connector_settings_class(conn_type: ConnectionType, settings_class: Type[ConnectorSettingsBase]) -> None:
    if (registered_settings_class := CONNECTORS_SETTINGS_CLASSES.get(conn_type)) is not None:
        assert registered_settings_class == settings_class
    else:
        CONNECTORS_SETTINGS_CLASSES[conn_type] = settings_class


def get_connector_settings_class(conn_type: ConnectionType) -> Type[ConnectorSettingsBase]:
    """Return class for given connection type"""
    return CONNECTORS_SETTINGS_CLASSES[conn_type]
