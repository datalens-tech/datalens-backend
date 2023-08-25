from bi_configs.connectors_settings import ConnectorSettingsBase, KonturMarketConnectorSettings, PartnerKeys
from bi_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from bi_configs.settings_loaders.meta_definition import required

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition


def kontur_market_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, ConnectorSettingsBase]:
    cfg = full_cfg.CONNECTORS_DATA
    return dict(
        KONTUR_MARKET=KonturMarketConnectorSettings(  # type: ignore
            HOST=cfg.KONTUR_MARKET.CONN_KONTUR_MARKET_HOST,
            PORT=cfg.KONTUR_MARKET.CONN_KONTUR_MARKET_PORT,
            USERNAME=cfg.KONTUR_MARKET.CONN_KONTUR_MARKET_USERNAME,
            USE_MANAGED_NETWORK=cfg.KONTUR_MARKET.CONN_KONTUR_MARKET_USE_MANAGED_NETWORK,
            PASSWORD=required(str),
            PARTNER_KEYS=required(PartnerKeys),
        )
    )


class KonturMarketSettingDefinition(ConnectorSettingsDefinition):
    settings_class = KonturMarketConnectorSettings
    fallback = kontur_market_settings_fallback
