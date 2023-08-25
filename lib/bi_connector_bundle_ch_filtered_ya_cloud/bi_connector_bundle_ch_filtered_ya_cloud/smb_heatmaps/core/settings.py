from bi_configs.connectors_settings import ConnectorSettingsBase, SMBHeatmapsConnectorSettings
from bi_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from bi_configs.settings_loaders.meta_definition import required

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition


def smb_heatmaps_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, ConnectorSettingsBase]:
    cfg = full_cfg.CONNECTORS_DATA
    return dict(
        SMB_HEATMAPS=SMBHeatmapsConnectorSettings(  # type: ignore
            HOST=cfg.SMB_HEATMAPS.CONN_SMB_HEATMAPS_HOST,
            PORT=cfg.SMB_HEATMAPS.CONN_SMB_HEATMAPS_PORT,
            DB_NAME=cfg.SMB_HEATMAPS.CONN_SMB_HEATMAPS_DB_MAME,
            USERNAME=cfg.SMB_HEATMAPS.CONN_SMB_HEATMAPS_USERNAME,
            USE_MANAGED_NETWORK=cfg.SMB_HEATMAPS.CONN_SMB_HEATMAPS_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.SMB_HEATMAPS.CONN_SMB_HEATMAPS_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.SMB_HEATMAPS.CONN_SMB_HEATMAPS_SUBSELECT_TEMPLATES),
            PASSWORD=required(str),
        )
    )


class CHSMBHeatmapsSettingDefinition(ConnectorSettingsDefinition):
    settings_class = SMBHeatmapsConnectorSettings
    fallback = smb_heatmaps_settings_fallback
