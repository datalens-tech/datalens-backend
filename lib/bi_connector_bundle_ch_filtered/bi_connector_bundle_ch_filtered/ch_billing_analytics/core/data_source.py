from __future__ import annotations

import logging

from bi_constants.enums import ConnectionType, CreateDSFrom

from bi_connector_bundle_ch_filtered.base.core.data_source import ClickHouseTemplatedSubselectDataSource

from bi_connector_bundle_ch_filtered.ch_billing_analytics.core.constants import (
    CONNECTION_TYPE_CH_BILLING_ANALYTICS,
    SOURCE_TYPE_CH_BILLING_ANALYTICS_TABLE,
)


LOGGER = logging.getLogger(__name__)


class BillingAnalyticsCHDataSource(ClickHouseTemplatedSubselectDataSource):
    """
    Clickhouse datasource with data filtration by current user YC Billing Accounts.
    """

    conn_type = CONNECTION_TYPE_CH_BILLING_ANALYTICS

    @classmethod
    def is_compatible_with_type(cls, source_type: CreateDSFrom) -> bool:
        return source_type in {
            SOURCE_TYPE_CH_BILLING_ANALYTICS_TABLE,
        }
