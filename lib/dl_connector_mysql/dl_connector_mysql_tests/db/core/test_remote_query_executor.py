import pytest

from dl_core_testing.testcases.remote_query_executor import BaseRemoteQueryExecutorTestClass

from dl_connector_mysql.core.adapters_mysql import MySQLAdapter
from dl_connector_mysql.core.async_adapters_mysql import AsyncMySQLAdapter
from dl_connector_mysql_tests.db.config import SUBSELECT_QUERY_FULL
from dl_connector_mysql_tests.db.core.base import BaseMySQLTestClass


class TestMySQLRemoteQueryExecutor(BaseMySQLTestClass, BaseRemoteQueryExecutorTestClass):
    SYNC_ADAPTER_CLS = MySQLAdapter
    ASYNC_ADAPTER_CLS = AsyncMySQLAdapter

    @pytest.mark.asyncio
    async def test_qe_result(self, remote_adapter):
        result = await self.execute_request(remote_adapter, query=SUBSELECT_QUERY_FULL)
        assert len(result) == 3
