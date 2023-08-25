from bi_configs.connectors_settings import ConnectorSettingsBase, MarketCouriersConnectorSettings
from bi_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from bi_configs.settings_loaders.meta_definition import required

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition


def market_couriers_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, ConnectorSettingsBase]:
    cfg = full_cfg.CONNECTORS_DATA
    return dict(
        MARKET_COURIERS=MarketCouriersConnectorSettings(  # type: ignore
            HOST=cfg.MARKET_COURIERS.CONN_MARKET_COURIERS_HOST,
            PORT=cfg.MARKET_COURIERS.CONN_MARKET_COURIERS_PORT,
            DB_NAME=cfg.MARKET_COURIERS.CONN_MARKET_COURIERS_DB_MAME,
            USERNAME=cfg.MARKET_COURIERS.CONN_MARKET_COURIERS_USERNAME,
            USE_MANAGED_NETWORK=cfg.MARKET_COURIERS.CONN_MARKET_COURIERS_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.MARKET_COURIERS.CONN_MARKET_COURIERS_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.MARKET_COURIERS.CONN_MARKET_COURIERS_SUBSELECT_TEMPLATES),
            PASSWORD=required(str),
        )
    )


class CHMarketCouriersSettingDefinition(ConnectorSettingsDefinition):
    settings_class = MarketCouriersConnectorSettings
    fallback = market_couriers_settings_fallback
