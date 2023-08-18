from __future__ import annotations

from typing import TYPE_CHECKING

from bi_connector_bundle_partners.base.core.us_connection import PartnersCHConnectionBase
from bi_connector_bundle_partners.kontur_market.core.constants import SOURCE_TYPE_KONTUR_MARKET_CH_TABLE

if TYPE_CHECKING:
    from bi_configs.connectors_settings import KonturMarketConnectorSettings
    from bi_core.us_manager.us_manager_sync import SyncUSManager


class KonturMarketCHConnection(PartnersCHConnectionBase):
    source_type = SOURCE_TYPE_KONTUR_MARKET_CH_TABLE
    allowed_source_types = frozenset((SOURCE_TYPE_KONTUR_MARKET_CH_TABLE,))

    @property
    def _connector_settings(self) -> KonturMarketConnectorSettings:
        settings = self._all_connectors_settings.KONTUR_MARKET
        assert settings is not None
        return settings

    @classmethod
    def _get_connector_settings(cls, usm: SyncUSManager) -> KonturMarketConnectorSettings:
        settings = cls._get_all_connectors_settings(usm).KONTUR_MARKET
        assert settings is not None
        return settings
