from bi_configs.connectors_settings import ConnectorsConfigType, ConnectorSettingsBase, CHFrozenDemoConnectorSettings
from bi_configs.settings_loaders.meta_definition import required
from bi_configs.connectors_data import ConnectorsDataCHFrozenDemoBase

from bi_constants.enums import RawSQLLevel

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition, get_connectors_settings_config


def ch_frozen_demo_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    cfg = get_connectors_settings_config(
        full_cfg, object_like_config_key='CH_FROZEN_DEMO', connector_data_class=ConnectorsDataCHFrozenDemoBase,
    )
    if cfg is None:
        return {}
    return dict(
        CH_FROZEN_DEMO=CHFrozenDemoConnectorSettings(  # type: ignore
            HOST=cfg.CONN_CH_FROZEN_DEMO_HOST,
            PORT=cfg.CONN_CH_FROZEN_DEMO_PORT,
            DB_NAME=cfg.CONN_CH_FROZEN_DEMO_DB_MAME,
            USERNAME=cfg.CONN_CH_FROZEN_DEMO_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_CH_FROZEN_DEMO_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CONN_CH_FROZEN_DEMO_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.CONN_CH_FROZEN_DEMO_SUBSELECT_TEMPLATES),  # type: ignore
            RAW_SQL_LEVEL=RawSQLLevel(cfg.CONN_CH_FROZEN_DEMO_RAW_SQL_LEVEL),
            PASS_DB_QUERY_TO_USER=cfg.CONN_CH_FROZEN_DEMO_PASS_DB_QUERY_TO_USER,
            PASSWORD=required(str),
        )
    )


class CHFrozenDemoSettingDefinition(ConnectorSettingsDefinition):
    settings_class = CHFrozenDemoConnectorSettings
    fallback = ch_frozen_demo_settings_fallback
