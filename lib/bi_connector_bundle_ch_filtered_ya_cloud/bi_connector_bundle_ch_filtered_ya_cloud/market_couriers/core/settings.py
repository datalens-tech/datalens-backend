from bi_configs.connectors_settings import ConnectorsConfigType, ConnectorSettingsBase, MarketCouriersConnectorSettings
from bi_configs.settings_loaders.meta_definition import required
from bi_configs.connectors_data import ConnectorsDataMarketCouriersBase

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition, get_connectors_settings_config


def market_couriers_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    cfg = get_connectors_settings_config(
        full_cfg, object_like_config_key='MARKET_COURIERS', connector_data_class=ConnectorsDataMarketCouriersBase,
    )
    if cfg is None:
        return {}
    return dict(
        MARKET_COURIERS=MarketCouriersConnectorSettings(  # type: ignore
            HOST=cfg.CONN_MARKET_COURIERS_HOST,
            PORT=cfg.CONN_MARKET_COURIERS_PORT,
            DB_NAME=cfg.CONN_MARKET_COURIERS_DB_MAME,
            USERNAME=cfg.CONN_MARKET_COURIERS_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_MARKET_COURIERS_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CONN_MARKET_COURIERS_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.CONN_MARKET_COURIERS_SUBSELECT_TEMPLATES),  # type: ignore
            PASSWORD=required(str),
        )
    )


class CHMarketCouriersSettingDefinition(ConnectorSettingsDefinition):
    settings_class = MarketCouriersConnectorSettings
    fallback = market_couriers_settings_fallback
