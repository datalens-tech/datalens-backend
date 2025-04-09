from dl_core_testing.testcases.adapter import BaseAsyncAdapterTestClass

from dl_connector_clickhouse.core.clickhouse.adapters import DLAsyncClickHouseAdapter
from dl_connector_clickhouse.core.clickhouse.target_dto import DLClickHouseConnTargetDTO
from dl_connector_clickhouse_tests.db.core.base import BaseClickHouseTestClass


class TestAsyncClickHouseAdapter(
    BaseClickHouseTestClass,
    BaseAsyncAdapterTestClass[DLClickHouseConnTargetDTO],
):
    ASYNC_ADAPTER_CLS = DLAsyncClickHouseAdapter
