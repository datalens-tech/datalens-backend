import pytest

from dl_core_testing.testcases.remote_query_executor import BaseRemoteQueryExecutorTestClass

from dl_connector_clickhouse.core.clickhouse_base.adapters import (
    AsyncClickHouseAdapter,
    ClickHouseAdapter,
)
from dl_connector_clickhouse_tests.db.config import DASHSQL_QUERY_FULL
from dl_connector_clickhouse_tests.db.core.base import BaseClickHouseTestClass


class TestClickHouseRemoteQueryExecutor(BaseClickHouseTestClass, BaseRemoteQueryExecutorTestClass):
    SYNC_ADAPTER_CLS = ClickHouseAdapter
    ASYNC_ADAPTER_CLS = AsyncClickHouseAdapter

    @pytest.mark.asyncio
    async def test_qe_result(self, remote_adapter):
        result = await self.execute_request(remote_adapter, query=DASHSQL_QUERY_FULL)
        assert len(result) == 7
