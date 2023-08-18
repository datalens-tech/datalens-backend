from __future__ import annotations

import logging

from bi_constants.enums import ConnectionType

from bi_core.connectors.clickhouse_base.data_source import ClickHouseTemplatedSubselectDataSource


LOGGER = logging.getLogger(__name__)


class BillingAnalyticsCHDataSource(ClickHouseTemplatedSubselectDataSource):
    """
    Clickhouse datasource with data filtration by current user YC Billing Accounts.
    """

    conn_type = ConnectionType.ch_billing_analytics
