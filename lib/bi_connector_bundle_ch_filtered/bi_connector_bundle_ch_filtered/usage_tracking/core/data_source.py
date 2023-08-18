from __future__ import annotations

import logging

from bi_constants.enums import ConnectionType

from bi_core.connectors.clickhouse_base.data_source import ClickHouseTemplatedSubselectDataSource


LOGGER = logging.getLogger(__name__)


class UsageTrackingDataSource(ClickHouseTemplatedSubselectDataSource):
    """
    Clickhouse datasource with data filtration by current tenant.
    """

    conn_type = ConnectionType.usage_tracking
