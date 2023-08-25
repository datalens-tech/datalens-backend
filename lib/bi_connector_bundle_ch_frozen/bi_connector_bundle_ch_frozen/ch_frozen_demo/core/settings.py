from bi_configs.connectors_settings import ConnectorSettingsBase, CHFrozenDemoConnectorSettings
from bi_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from bi_configs.settings_loaders.meta_definition import required
from bi_constants.enums import RawSQLLevel

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition


def ch_frozen_demo_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, ConnectorSettingsBase]:
    cfg = full_cfg.CONNECTORS_DATA
    return dict(
        CH_FROZEN_DEMO=CHFrozenDemoConnectorSettings(
            HOST=cfg.CH_FROZEN_DEMO.CONN_CH_FROZEN_DEMO_HOST,
            PORT=cfg.CH_FROZEN_DEMO.CONN_CH_FROZEN_DEMO_PORT,
            DB_NAME=cfg.CH_FROZEN_DEMO.CONN_CH_FROZEN_DEMO_DB_MAME,
            USERNAME=cfg.CH_FROZEN_DEMO.CONN_CH_FROZEN_DEMO_USERNAME,
            USE_MANAGED_NETWORK=cfg.CH_FROZEN_DEMO.CONN_CH_FROZEN_DEMO_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CH_FROZEN_DEMO.CONN_CH_FROZEN_DEMO_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.CH_FROZEN_DEMO.CONN_CH_FROZEN_DEMO_SUBSELECT_TEMPLATES),
            RAW_SQL_LEVEL=RawSQLLevel(cfg.CH_FROZEN_DEMO.CONN_CH_FROZEN_DEMO_RAW_SQL_LEVEL),
            PASS_DB_QUERY_TO_USER=cfg.CH_FROZEN_DEMO.CONN_CH_FROZEN_DEMO_PASS_DB_QUERY_TO_USER,
            PASSWORD=required(str),
        )
    )


class CHFrozenDemoSettingDefinition(ConnectorSettingsDefinition):
    settings_class = CHFrozenDemoConnectorSettings
    fallback = ch_frozen_demo_settings_fallback
