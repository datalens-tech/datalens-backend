from typing import ClassVar, Optional

from bi_configs.connectors_settings import (
    ConnectorsConfigType, ConnectorSettingsBase, KonturMarketConnectorSettings, PartnerKeys,
)
from bi_configs.settings_loaders.meta_definition import required
from bi_configs.connectors_data import ConnectorsDataBase

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition, get_connectors_settings_config


class ConnectorsDataKonturMarketBase(ConnectorsDataBase):
    CONN_KONTUR_MARKET_HOST: ClassVar[Optional[str]] = None
    CONN_KONTUR_MARKET_PORT: ClassVar[Optional[int]] = None
    CONN_KONTUR_MARKET_USERNAME: ClassVar[Optional[str]] = None
    CONN_KONTUR_MARKET_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = None

    @classmethod
    def connector_name(cls) -> str:
        return 'KONTUR_MARKET'


def kontur_market_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    cfg = get_connectors_settings_config(
        full_cfg, object_like_config_key='KONTUR_MARKET', connector_data_class=ConnectorsDataKonturMarketBase,
    )
    if cfg is None:
        return {}
    return dict(
        KONTUR_MARKET=KonturMarketConnectorSettings(  # type: ignore
            HOST=cfg.CONN_KONTUR_MARKET_HOST,
            PORT=cfg.CONN_KONTUR_MARKET_PORT,
            USERNAME=cfg.CONN_KONTUR_MARKET_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_KONTUR_MARKET_USE_MANAGED_NETWORK,
            PASSWORD=required(str),
            PARTNER_KEYS=required(PartnerKeys),
        )
    )


class KonturMarketSettingDefinition(ConnectorSettingsDefinition):
    settings_class = KonturMarketConnectorSettings
    fallback = kontur_market_settings_fallback
