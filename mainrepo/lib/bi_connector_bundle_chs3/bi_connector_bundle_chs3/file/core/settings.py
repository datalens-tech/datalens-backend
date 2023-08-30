from bi_configs.connectors_settings import ConnectorsConfigType, ConnectorSettingsBase, FileS3ConnectorSettings
from bi_configs.settings_loaders.meta_definition import required
from bi_configs.connectors_data import ConnectorsDataFileBase

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition, get_connectors_settings_config


def file_s3_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    cfg = get_connectors_settings_config(
        full_cfg, object_like_config_key='FILE', connector_data_class=ConnectorsDataFileBase,
    )
    if cfg is None:
        return {}
    return dict(
        FILE=FileS3ConnectorSettings(  # type: ignore
            HOST=cfg.CONN_FILE_CH_HOST,
            PORT=cfg.CONN_FILE_CH_PORT,
            USERNAME=cfg.CONN_FILE_CH_USERNAME,
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
