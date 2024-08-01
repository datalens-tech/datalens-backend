import pytest

from dl_core_testing.testcases.remote_query_executor import BaseRemoteQueryExecutorTestClass

from dl_connector_mssql.core.adapters_mssql import MSSQLDefaultAdapter
from dl_connector_mssql_tests.db.config import SUBSELECT_QUERY_FULL
from dl_connector_mssql_tests.db.core.base import BaseMSSQLTestClass


class TestMSSQLRemoteQueryExecutor(BaseMSSQLTestClass, BaseRemoteQueryExecutorTestClass):
    ADAPTER_CLS = MSSQLDefaultAdapter

    @pytest.mark.asyncio
    async def test_qe_result(self, remote_adapter):
        result = await self.execute_request(remote_adapter, query=SUBSELECT_QUERY_FULL)
        assert len(result) == 3
