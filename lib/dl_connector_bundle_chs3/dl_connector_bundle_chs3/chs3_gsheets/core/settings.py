from typing import ClassVar

from dl_configs.connectors_settings import DeprecatedConnectorSettingsBase
from dl_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from dl_configs.settings_loaders.meta_definition import required
from dl_core.connectors.settings.base import ConnectorSettings
from dl_core.connectors.settings.primitives import (
    ConnectorSettingsDefinition,
    get_connectors_settings_config,
)

from dl_connector_bundle_chs3.chs3_base.core.settings import (
    DeprecatedFileS3ConnectorSettings,
    FileS3ConnectorSettingsBase,
)
from dl_connector_bundle_chs3.chs3_gsheets.core.constants import CONNECTION_TYPE_GSHEETS_V2


def gsheets_file_s3_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, DeprecatedConnectorSettingsBase]:
    cfg = get_connectors_settings_config(full_cfg, object_like_config_key="FILE")
    if cfg is None:
        return {}
    return dict(
        GSHEETS_V2=DeprecatedFileS3ConnectorSettings(
            HOST=cfg.CONN_FILE_CH_HOST,
            PORT=cfg.CONN_FILE_CH_PORT,
            USERNAME=cfg.CONN_FILE_CH_USERNAME,
            PASSWORD=required(str),
            ACCESS_KEY_ID=required(str),
            SECRET_ACCESS_KEY=required(str),
            S3_ENDPOINT=full_cfg.S3_ENDPOINT_URL,  # type: ignore  # 2024-01-24 # TODO: Item "LegacyDefaults" of "ObjectLikeConfig | LegacyDefaults" has no attribute "S3_ENDPOINT_URL"  [union-attr]
            BUCKET=full_cfg.FILE_UPLOADER_S3_PERSISTENT_BUCKET_NAME,
        )
    )


class GSheetsConnectorSettings(ConnectorSettings, FileS3ConnectorSettingsBase):
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
    }


class GSheetsFileS3SettingDefinition(ConnectorSettingsDefinition):
    settings_class = DeprecatedFileS3ConnectorSettings
    fallback = gsheets_file_s3_settings_fallback

    pydantic_settings_class = GSheetsConnectorSettings
