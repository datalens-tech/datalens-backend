from typing import ClassVar

from dl_core.connectors.settings.primitives import ConnectorSettingsDefinition

from dl_connector_bundle_chs3.chs3_base.core.settings import FileS3ConnectorSettingsBase
from dl_connector_bundle_chs3.chs3_gsheets.core.constants import CONNECTION_TYPE_GSHEETS_V2


class GSheetsConnectorSettings(FileS3ConnectorSettingsBase):
    type: str = CONNECTION_TYPE_GSHEETS_V2.value

    root_fallback_env_keys: ClassVar[dict[str, str]] = {
        "CONNECTORS__GSHEETS_V2__SECURE": "CONNECTORS_GSHEETS_V2_SECURE",
        "CONNECTORS__GSHEETS_V2__HOST": "CONNECTORS_GSHEETS_V2_HOST",
        "CONNECTORS__GSHEETS_V2__PORT": "CONNECTORS_GSHEETS_V2_PORT",
        "CONNECTORS__GSHEETS_V2__USERNAME": "CONNECTORS_GSHEETS_V2_USERNAME",
        "CONNECTORS__GSHEETS_V2__PASSWORD": "CONNECTORS_GSHEETS_V2_PASSWORD",
        "CONNECTORS__GSHEETS_V2__ACCESS_KEY_ID": "CONNECTORS_GSHEETS_V2_ACCESS_KEY_ID",
        "CONNECTORS__GSHEETS_V2__SECRET_ACCESS_KEY": "CONNECTORS_GSHEETS_V2_SECRET_ACCESS_KEY",
        "CONNECTORS__GSHEETS_V2__REPLACE_SECRET_SALT": "CONNECTORS_GSHEETS_V2_REPLACE_SECRET_SALT",
        "CONNECTORS__GSHEETS_V2__BUCKET": "CONNECTORS_GSHEETS_V2_BUCKET",
        "CONNECTORS__GSHEETS_V2__S3_ENDPOINT": "CONNECTORS_GSHEETS_V2_S3_ENDPOINT",
    }


class GSheetsFileS3SettingDefinition(ConnectorSettingsDefinition):
    pydantic_settings_class = GSheetsConnectorSettings
