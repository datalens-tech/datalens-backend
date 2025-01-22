from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from dl_configs.settings_loaders.meta_definition import required
from dl_core.connectors.settings.primitives import (
    ConnectorSettingsDefinition,
    get_connectors_settings_config,
)

from dl_connector_bundle_chs3.chs3_base.core.settings import FileS3ConnectorSettings


def gsheets_file_s3_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, ConnectorSettingsBase]:
    cfg = get_connectors_settings_config(full_cfg, object_like_config_key="FILE")
    if cfg is None:
        return {}
    return dict(
        GSHEETS_V2=FileS3ConnectorSettings(
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


class GSheetsFileS3SettingDefinition(ConnectorSettingsDefinition):
    settings_class = FileS3ConnectorSettings
    fallback = gsheets_file_s3_settings_fallback
