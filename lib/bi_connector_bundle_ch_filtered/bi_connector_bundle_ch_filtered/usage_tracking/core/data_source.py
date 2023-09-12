from __future__ import annotations

import logging

from bi_constants.enums import CreateDSFrom

from bi_connector_bundle_ch_filtered.base.core.data_source import ClickHouseTemplatedSubselectDataSource

from bi_connector_bundle_ch_filtered.usage_tracking.core.constants import (
    CONNECTION_TYPE_USAGE_TRACKING,
    SOURCE_TYPE_CH_USAGE_TRACKING_TABLE,
)


LOGGER = logging.getLogger(__name__)


class UsageTrackingDataSource(ClickHouseTemplatedSubselectDataSource):
    """
    Clickhouse datasource with data filtration by current tenant.
    """

    conn_type = CONNECTION_TYPE_USAGE_TRACKING

    @classmethod
    def is_compatible_with_type(cls, source_type: CreateDSFrom) -> bool:
        return source_type in {
            SOURCE_TYPE_CH_USAGE_TRACKING_TABLE,
        }
