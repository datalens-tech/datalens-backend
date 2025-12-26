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
from dl_connector_bundle_chs3.chs3_yadocs.core.constants import CONNECTION_TYPE_YADOCS


def yadocs_file_s3_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, DeprecatedConnectorSettingsBase]:
    cfg = get_connectors_settings_config(full_cfg, object_like_config_key="FILE")
    if cfg is None:
        return {}
    return dict(
        YADOCS=DeprecatedFileS3ConnectorSettings(
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


class YaDocsConnectorSettings(ConnectorSettings, FileS3ConnectorSettingsBase):
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
    }


class YaDocsFileS3SettingDefinition(ConnectorSettingsDefinition):
    settings_class = DeprecatedFileS3ConnectorSettings
    fallback = yadocs_file_s3_settings_fallback

    pydantic_settings_class = YaDocsConnectorSettings
