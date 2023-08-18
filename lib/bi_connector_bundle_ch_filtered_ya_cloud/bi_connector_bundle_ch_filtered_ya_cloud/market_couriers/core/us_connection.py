from __future__ import annotations

from typing import ClassVar, TYPE_CHECKING

from bi_connector_bundle_ch_filtered_ya_cloud.market_couriers.core.constants import (
    SOURCE_TYPE_CH_MARKET_COURIERS_TABLE,
)
from bi_connector_bundle_ch_filtered_ya_cloud.base.core.us_connection import ConnectionCHFilteredSubselectByPuidBase

if TYPE_CHECKING:
    from bi_configs.connectors_settings import MarketCouriersConnectorSettings


class ConnectionClickhouseMarketCouriers(ConnectionCHFilteredSubselectByPuidBase):
    source_type = SOURCE_TYPE_CH_MARKET_COURIERS_TABLE
    allowed_source_types = frozenset((SOURCE_TYPE_CH_MARKET_COURIERS_TABLE,))
    is_always_internal_source: ClassVar[bool] = True
    allow_cache: ClassVar[bool] = True

    @property
    def _connector_settings(self) -> MarketCouriersConnectorSettings:
        settings = self._all_connectors_settings.MARKET_COURIERS
        assert settings is not None
        return settings
