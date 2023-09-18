from __future__ import annotations

import logging
from typing import ClassVar

from dl_constants.enums import CreateDSFrom

from bi_connector_bundle_ch_filtered.base.core.data_source import ClickHouseTemplatedSubselectDataSource
from bi_connector_bundle_ch_filtered_ya_cloud.market_couriers.core.constants import (
    CONNECTION_TYPE_MARKET_COURIERS,
    SOURCE_TYPE_CH_MARKET_COURIERS_TABLE,
)

LOGGER = logging.getLogger(__name__)


class ClickHouseMarketCouriersDataSource(ClickHouseTemplatedSubselectDataSource):
    """
    Clickhouse datasource with data filtration by Yandex Passport userId of the connection creator.
    """

    preview_enabled: ClassVar[bool] = False

    conn_type = CONNECTION_TYPE_MARKET_COURIERS

    @classmethod
    def is_compatible_with_type(cls, source_type: CreateDSFrom) -> bool:
        return source_type in {
            SOURCE_TYPE_CH_MARKET_COURIERS_TABLE,
        }
