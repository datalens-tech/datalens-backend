from dl_core.connectors.settings.base import ConnectorSettings

from dl_connector_bundle_chs3.file.core.constants import CONNECTION_TYPE_FILE
from dl_connector_bundle_chs3.file.core.settings import FileConnectorSettings


CONNECTORS_SETTINGS_ROOT_FALLBACK_ENV_KEYS: dict[str, str] = {}


def register_file_uploader_worker_conn_settings() -> None:
    ConnectorSettings.register(CONNECTION_TYPE_FILE.value, FileConnectorSettings)
    CONNECTORS_SETTINGS_ROOT_FALLBACK_ENV_KEYS.update(FileConnectorSettings.root_fallback_env_keys)
