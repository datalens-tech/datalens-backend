from bi_configs.connectors_settings import ConnectorSettingsBase, FileS3ConnectorSettings
from bi_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from bi_configs.settings_loaders.meta_definition import required

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition


def file_s3_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, ConnectorSettingsBase]:
    cfg = full_cfg.CONNECTORS_DATA
    return dict(
        FILE=FileS3ConnectorSettings(  # type: ignore
            HOST=cfg.FILE.CONN_FILE_CH_HOST,
            PORT=cfg.FILE.CONN_FILE_CH_PORT,
            USERNAME=cfg.FILE.CONN_FILE_CH_USERNAME,
            PASSWORD=required(str),
            ACCESS_KEY_ID=required(str),
            SECRET_ACCESS_KEY=required(str),
            S3_ENDPOINT=full_cfg.S3_ENDPOINT_URL,
            BUCKET=full_cfg.FILE_UPLOADER_S3_PERSISTENT_BUCKET_NAME,
        )
    )


class FileS3SettingDefinition(ConnectorSettingsDefinition):
    settings_class = FileS3ConnectorSettings
    fallback = file_s3_settings_fallback
