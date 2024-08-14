import pytest

from dl_core_testing.testcases.remote_query_executor import BaseRemoteQueryExecutorTestClass

from dl_connector_postgresql.core.postgresql_base.adapters_postgres import PostgresAdapter
from dl_connector_postgresql.core.postgresql_base.async_adapters_postgres import AsyncPostgresAdapter
from dl_connector_postgresql_tests.db.config import SUBSELECT_QUERY_FULL
from dl_connector_postgresql_tests.db.core.base import BasePostgreSQLTestClass


class TestPostgreSQLRemoteQueryExecutor(BasePostgreSQLTestClass, BaseRemoteQueryExecutorTestClass):
    SYNC_ADAPTER_CLS = PostgresAdapter
    ASYNC_ADAPTER_CLS = AsyncPostgresAdapter

    @pytest.mark.asyncio
    async def test_qe_result(self, remote_adapter):
        result = await self.execute_request(remote_adapter, query=SUBSELECT_QUERY_FULL)
        assert len(result) == 7
