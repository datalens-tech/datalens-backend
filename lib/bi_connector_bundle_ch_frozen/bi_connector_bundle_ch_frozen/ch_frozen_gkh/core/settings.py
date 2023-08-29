from bi_configs.connectors_settings import ConnectorsConfigType, ConnectorSettingsBase, CHFrozenGKHConnectorSettings
from bi_configs.settings_loaders.meta_definition import required
from bi_defaults.connectors_data import ConnectorsDataCHFrozenGKHBase

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition, get_connectors_settings_config


def ch_frozen_gkh_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    cfg = get_connectors_settings_config(
        full_cfg, object_like_config_key='CH_FROZEN_GKH', connector_data_class=ConnectorsDataCHFrozenGKHBase,
    )
    if cfg is None:
        return {}
    return dict(
        CH_FROZEN_GKH=CHFrozenGKHConnectorSettings(  # type: ignore
            HOST=cfg.CONN_CH_FROZEN_GKH_HOST,
            PORT=cfg.CONN_CH_FROZEN_GKH_PORT,
            DB_NAME=cfg.CONN_CH_FROZEN_GKH_DB_MAME,
            USERNAME=cfg.CONN_CH_FROZEN_GKH_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_CH_FROZEN_GKH_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CONN_CH_FROZEN_GKH_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.CONN_CH_FROZEN_GKH_SUBSELECT_TEMPLATES),  # type: ignore
            PASSWORD=required(str),
        )
    )


class CHFrozenGKHSettingDefinition(ConnectorSettingsDefinition):
    settings_class = CHFrozenGKHConnectorSettings
    fallback = ch_frozen_gkh_settings_fallback
