from dl_core_testing.testcases.adapter import BaseAsyncAdapterTestClass

from dl_connector_clickhouse.core.clickhouse_base.adapters import AsyncClickHouseAdapter
from dl_connector_clickhouse.core.clickhouse_base.target_dto import ClickHouseConnTargetDTO
from dl_connector_clickhouse_tests.db.core.base import BaseClickHouseTestClass


class TestAsyncClickHouseAdapter(
    BaseClickHouseTestClass,
    BaseAsyncAdapterTestClass[ClickHouseConnTargetDTO],
):
    ASYNC_ADAPTER_CLS = AsyncClickHouseAdapter
