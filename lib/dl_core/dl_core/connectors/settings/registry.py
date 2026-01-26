from dl_constants.enums import ConnectionType
from dl_core.connectors.settings.base import ConnectorSettings


CONNECTORS_SETTINGS_ROOT_FALLBACK_ENV_KEYS: dict[str, str] = {}


def register_connector_settings_class(
    conn_type: ConnectionType,
    pydantic_settings_class: type[ConnectorSettings],
) -> None:
    ConnectorSettings.register(conn_type.value, pydantic_settings_class)
    CONNECTORS_SETTINGS_ROOT_FALLBACK_ENV_KEYS.update(pydantic_settings_class.root_fallback_env_keys)
