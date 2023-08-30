from __future__ import annotations

from typing import ClassVar

from bi_configs.connectors_settings import MarketCouriersConnectorSettings

from bi_connector_bundle_ch_filtered_ya_cloud.market_couriers.core.constants import (
    SOURCE_TYPE_CH_MARKET_COURIERS_TABLE,
)
from bi_connector_bundle_ch_filtered_ya_cloud.base.core.us_connection import ConnectionCHFilteredSubselectByPuidBase


class ConnectionClickhouseMarketCouriers(
        ConnectionCHFilteredSubselectByPuidBase[MarketCouriersConnectorSettings]
):
    source_type = SOURCE_TYPE_CH_MARKET_COURIERS_TABLE
    allowed_source_types = frozenset((SOURCE_TYPE_CH_MARKET_COURIERS_TABLE,))
    is_always_internal_source: ClassVar[bool] = True
    allow_cache: ClassVar[bool] = True
    settings_type = MarketCouriersConnectorSettings
