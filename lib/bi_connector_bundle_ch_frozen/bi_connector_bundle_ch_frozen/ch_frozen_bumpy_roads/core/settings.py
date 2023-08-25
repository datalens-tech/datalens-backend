from bi_configs.connectors_settings import ConnectorSettingsBase, CHFrozenBumpyRoadsConnectorSettings
from bi_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from bi_configs.settings_loaders.meta_definition import required

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition


def ch_frozen_bumpy_roads_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, ConnectorSettingsBase]:
    cfg = full_cfg.CONNECTORS_DATA
    return dict(
        CH_FROZEN_BUMPY_ROADS=CHFrozenBumpyRoadsConnectorSettings(
            HOST=cfg.CH_FROZEN_BUMPY_ROADS.CONN_CH_FROZEN_BUMPY_ROADS_HOST,
            PORT=cfg.CH_FROZEN_BUMPY_ROADS.CONN_CH_FROZEN_BUMPY_ROADS_PORT,
            DB_NAME=cfg.CH_FROZEN_BUMPY_ROADS.CONN_CH_FROZEN_BUMPY_ROADS_DB_MAME,
            USERNAME=cfg.CH_FROZEN_BUMPY_ROADS.CONN_CH_FROZEN_BUMPY_ROADS_USERNAME,
            USE_MANAGED_NETWORK=cfg.CH_FROZEN_BUMPY_ROADS.CONN_CH_FROZEN_BUMPY_ROADS_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CH_FROZEN_BUMPY_ROADS.CONN_CH_FROZEN_BUMPY_ROADS_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.CH_FROZEN_BUMPY_ROADS.CONN_CH_FROZEN_BUMPY_ROADS_SUBSELECT_TEMPLATES),
            PASSWORD=required(str),
        )
    )


class CHFrozenBumpyRoadsSettingDefinition(ConnectorSettingsDefinition):
    settings_class = CHFrozenBumpyRoadsConnectorSettings
    fallback = ch_frozen_bumpy_roads_settings_fallback
