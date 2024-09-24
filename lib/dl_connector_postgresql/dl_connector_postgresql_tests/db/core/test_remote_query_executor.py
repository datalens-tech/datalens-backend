import pytest

from dl_core.exc import SourceTimeout
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

    @pytest.mark.asyncio
    @pytest.mark.parametrize("forbid_private_addr", [True, False])
    async def test_forbid_private_hosts(self, remote_adapter, forbid_private_addr):
        if forbid_private_addr:
            async with remote_adapter:
                with pytest.raises(SourceTimeout, match=r"Source timed out \(DB query: select 1\)"):
                    await self.execute_request(remote_adapter, query="select 1")
        else:
            result = await self.execute_request(remote_adapter, query="select 1")
            assert result[0][0] == 1
