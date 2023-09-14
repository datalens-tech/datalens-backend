from __future__ import annotations

from bi_connector_clickhouse.core.clickhouse.data_source import ClickHouseDataSource, ClickHouseSubselectDataSource


class ClickHouseMDBDataSource(ClickHouseDataSource):
    """ MDB CH table """


class ClickHouseMDBSubselectDataSource(ClickHouseSubselectDataSource):
    """ MDB CH subselect """
