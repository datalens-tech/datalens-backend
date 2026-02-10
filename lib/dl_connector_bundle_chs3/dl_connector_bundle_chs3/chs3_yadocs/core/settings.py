from typing import ClassVar

from dl_core.connectors.settings.primitives import ConnectorSettingsDefinition

from dl_connector_bundle_chs3.chs3_base.core.settings import FileS3ConnectorSettingsBase
from dl_connector_bundle_chs3.chs3_yadocs.core.constants import CONNECTION_TYPE_YADOCS


class YaDocsConnectorSettings(FileS3ConnectorSettingsBase):
    type: str = CONNECTION_TYPE_YADOCS.value

    root_fallback_env_keys: ClassVar[dict[str, str]] = {
        "CONNECTORS__YADOCS__SECURE": "CONNECTORS_YADOCS_SECURE",
        "CONNECTORS__YADOCS__HOST": "CONNECTORS_YADOCS_HOST",
        "CONNECTORS__YADOCS__PORT": "CONNECTORS_YADOCS_PORT",
        "CONNECTORS__YADOCS__USERNAME": "CONNECTORS_YADOCS_USERNAME",
        "CONNECTORS__YADOCS__PASSWORD": "CONNECTORS_YADOCS_PASSWORD",
        "CONNECTORS__YADOCS__ACCESS_KEY_ID": "CONNECTORS_YADOCS_ACCESS_KEY_ID",
        "CONNECTORS__YADOCS__SECRET_ACCESS_KEY": "CONNECTORS_YADOCS_SECRET_ACCESS_KEY",
        "CONNECTORS__YADOCS__REPLACE_SECRET_SALT": "CONNECTORS_YADOCS_REPLACE_SECRET_SALT",
        "CONNECTORS__YADOCS__BUCKET": "CONNECTORS_YADOCS_BUCKET",
        "CONNECTORS__YADOCS__S3_ENDPOINT": "CONNECTORS_YADOCS_S3_ENDPOINT",
    }


class YaDocsFileS3SettingDefinition(ConnectorSettingsDefinition):
    pydantic_settings_class = YaDocsConnectorSettings
