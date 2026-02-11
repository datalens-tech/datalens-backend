from typing import ClassVar

from dl_core.connectors.settings.primitives import ConnectorSettingsDefinition

from dl_connector_bundle_chs3.chs3_base.core.settings import FileS3ConnectorSettingsBase


class FileConnectorSettings(FileS3ConnectorSettingsBase):
    root_fallback_env_keys: ClassVar[dict[str, str]] = {
        "CONNECTORS__FILE__SECURE": "CONNECTORS_FILE_SECURE",
        "CONNECTORS__FILE__HOST": "CONNECTORS_FILE_HOST",
        "CONNECTORS__FILE__PORT": "CONNECTORS_FILE_PORT",
        "CONNECTORS__FILE__USERNAME": "CONNECTORS_FILE_USERNAME",
        "CONNECTORS__FILE__PASSWORD": "CONNECTORS_FILE_PASSWORD",
        "CONNECTORS__FILE__ACCESS_KEY_ID": "CONNECTORS_FILE_ACCESS_KEY_ID",
        "CONNECTORS__FILE__SECRET_ACCESS_KEY": "CONNECTORS_FILE_SECRET_ACCESS_KEY",
        "CONNECTORS__FILE__REPLACE_SECRET_SALT": "CONNECTORS_FILE_REPLACE_SECRET_SALT",
        "CONNECTORS__FILE__BUCKET": "CONNECTORS_FILE_BUCKET",
        "CONNECTORS__FILE__S3_ENDPOINT": "CONNECTORS_FILE_S3_ENDPOINT",
    }


class FileS3SettingDefinition(ConnectorSettingsDefinition):
    pydantic_settings_class = FileConnectorSettings
