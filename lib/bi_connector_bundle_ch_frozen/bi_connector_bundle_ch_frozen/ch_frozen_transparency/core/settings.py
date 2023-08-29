from bi_configs.connectors_settings import (
    ConnectorsConfigType, ConnectorSettingsBase, CHFrozenTransparencyConnectorSettings,
)
from bi_configs.settings_loaders.meta_definition import required
from bi_defaults.connectors_data import ConnectorsDataCHFrozenTransparencyBase

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition, get_connectors_settings_config


def ch_frozen_transparency_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    cfg = get_connectors_settings_config(
        full_cfg, object_like_config_key='CH_FROZEN_TRANSPARENCY',
        connector_data_class=ConnectorsDataCHFrozenTransparencyBase,
    )
    if cfg is None:
        return {}
    return dict(
        CH_FROZEN_TRANSPARENCY=CHFrozenTransparencyConnectorSettings(  # type: ignore
            HOST=cfg.CONN_CH_FROZEN_TRANSPARENCY_HOST,
            PORT=cfg.CONN_CH_FROZEN_TRANSPARENCY_PORT,
            DB_NAME=cfg.CONN_CH_FROZEN_TRANSPARENCY_DB_MAME,
            USERNAME=cfg.CONN_CH_FROZEN_TRANSPARENCY_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_CH_FROZEN_TRANSPARENCY_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CONN_CH_FROZEN_TRANSPARENCY_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.CONN_CH_FROZEN_TRANSPARENCY_SUBSELECT_TEMPLATES),  # type: ignore
            PASSWORD=required(str),
        )
    )


class CHFrozenTransparencySettingDefinition(ConnectorSettingsDefinition):
    settings_class = CHFrozenTransparencyConnectorSettings
    fallback = ch_frozen_transparency_settings_fallback
