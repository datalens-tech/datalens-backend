from bi_configs.connectors_settings import ConnectorSettingsBase, CHFrozenGKHConnectorSettings
from bi_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from bi_configs.settings_loaders.meta_definition import required

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition


def ch_frozen_gkh_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, ConnectorSettingsBase]:
    cfg = full_cfg.CONNECTORS_DATA
    return dict(
        CH_FROZEN_GKH=CHFrozenGKHConnectorSettings(
            HOST=cfg.CH_FROZEN_GKH.CONN_CH_FROZEN_GKH_HOST,
            PORT=cfg.CH_FROZEN_GKH.CONN_CH_FROZEN_GKH_PORT,
            DB_NAME=cfg.CH_FROZEN_GKH.CONN_CH_FROZEN_GKH_DB_MAME,
            USERNAME=cfg.CH_FROZEN_GKH.CONN_CH_FROZEN_GKH_USERNAME,
            USE_MANAGED_NETWORK=cfg.CH_FROZEN_GKH.CONN_CH_FROZEN_GKH_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CH_FROZEN_GKH.CONN_CH_FROZEN_GKH_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.CH_FROZEN_GKH.CONN_CH_FROZEN_GKH_SUBSELECT_TEMPLATES),
            PASSWORD=required(str),
        )
    )


class CHFrozenGKHSettingDefinition(ConnectorSettingsDefinition):
    settings_class = CHFrozenGKHConnectorSettings
    fallback = ch_frozen_gkh_settings_fallback
