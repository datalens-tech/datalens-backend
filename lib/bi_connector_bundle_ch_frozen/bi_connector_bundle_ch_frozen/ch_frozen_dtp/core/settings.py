from bi_configs.connectors_settings import ConnectorsConfigType, ConnectorSettingsBase, CHFrozenDTPConnectorSettings
from bi_configs.settings_loaders.meta_definition import required
from bi_configs.connectors_data import ConnectorsDataCHFrozenDTPBase

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition, get_connectors_settings_config


def ch_frozen_dtp_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    cfg = get_connectors_settings_config(
        full_cfg, object_like_config_key='CH_FROZEN_DTP', connector_data_class=ConnectorsDataCHFrozenDTPBase,
    )
    if cfg is None:
        return {}
    return dict(
        CH_FROZEN_DTP=CHFrozenDTPConnectorSettings(  # type: ignore
            HOST=cfg.CONN_CH_FROZEN_DTP_HOST,
            PORT=cfg.CONN_CH_FROZEN_DTP_PORT,
            DB_NAME=cfg.CONN_CH_FROZEN_DTP_DB_MAME,
            USERNAME=cfg.CONN_CH_FROZEN_DTP_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_CH_FROZEN_DTP_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CONN_CH_FROZEN_DTP_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.CONN_CH_FROZEN_DTP_SUBSELECT_TEMPLATES),  # type: ignore
            PASSWORD=required(str),
        )
    )


class CHFrozenDTPSettingDefinition(ConnectorSettingsDefinition):
    settings_class = CHFrozenDTPConnectorSettings
    fallback = ch_frozen_dtp_settings_fallback
