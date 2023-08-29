from bi_configs.connectors_settings import (
    ConnectorsConfigType, ConnectorSettingsBase, CHFrozenBumpyRoadsConnectorSettings,
)
from bi_configs.settings_loaders.meta_definition import required
from bi_defaults.connectors_data import ConnectorsDataCHFrozenBumpyRoadsBase

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition, get_connectors_settings_config


def ch_frozen_bumpy_roads_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    cfg = get_connectors_settings_config(
        full_cfg, object_like_config_key='CH_FROZEN_BUMPY_ROADS',
        connector_data_class=ConnectorsDataCHFrozenBumpyRoadsBase,
    )
    if cfg is None:
        return {}
    return dict(
        CH_FROZEN_BUMPY_ROADS=CHFrozenBumpyRoadsConnectorSettings(  # type: ignore
            HOST=cfg.CONN_CH_FROZEN_BUMPY_ROADS_HOST,
            PORT=cfg.CONN_CH_FROZEN_BUMPY_ROADS_PORT,
            DB_NAME=cfg.CONN_CH_FROZEN_BUMPY_ROADS_DB_MAME,
            USERNAME=cfg.CONN_CH_FROZEN_BUMPY_ROADS_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_CH_FROZEN_BUMPY_ROADS_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CONN_CH_FROZEN_BUMPY_ROADS_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.CONN_CH_FROZEN_BUMPY_ROADS_SUBSELECT_TEMPLATES),  # type: ignore
            PASSWORD=required(str),
        )
    )


class CHFrozenBumpyRoadsSettingDefinition(ConnectorSettingsDefinition):
    settings_class = CHFrozenBumpyRoadsConnectorSettings
    fallback = ch_frozen_bumpy_roads_settings_fallback
