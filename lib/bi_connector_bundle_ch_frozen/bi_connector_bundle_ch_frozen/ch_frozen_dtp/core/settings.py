from bi_configs.connectors_settings import ConnectorSettingsBase, CHFrozenDTPConnectorSettings
from bi_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from bi_configs.settings_loaders.meta_definition import required

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition


def ch_frozen_dtp_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, ConnectorSettingsBase]:
    cfg = full_cfg.CONNECTORS_DATA
    return dict(
        CH_FROZEN_DTP=CHFrozenDTPConnectorSettings(
            HOST=cfg.CH_FROZEN_DTP.CONN_CH_FROZEN_DTP_HOST,
            PORT=cfg.CH_FROZEN_DTP.CONN_CH_FROZEN_DTP_PORT,
            DB_NAME=cfg.CH_FROZEN_DTP.CONN_CH_FROZEN_DTP_DB_MAME,
            USERNAME=cfg.CH_FROZEN_DTP.CONN_CH_FROZEN_DTP_USERNAME,
            USE_MANAGED_NETWORK=cfg.CH_FROZEN_DTP.CONN_CH_FROZEN_DTP_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CH_FROZEN_DTP.CONN_CH_FROZEN_DTP_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.CH_FROZEN_DTP.CONN_CH_FROZEN_DTP_SUBSELECT_TEMPLATES),
            PASSWORD=required(str),
        )
    )


class CHFrozenDTPSettingDefinition(ConnectorSettingsDefinition):
    settings_class = CHFrozenDTPConnectorSettings
    fallback = ch_frozen_dtp_settings_fallback
