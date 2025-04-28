import pytest

from dl_core.connection_executors.adapters.async_adapters_remote import RemoteAsyncAdapter
from dl_core.connection_models import TableIdent
from dl_core_testing.testcases.remote_query_executor import BaseRemoteQueryExecutorTestClass

from dl_connector_trino.core.adapters import TrinoDefaultAdapter
from dl_connector_trino_tests.db.config import SUBSELECT_QUERY
from dl_connector_trino_tests.db.core.base import BaseTrinoTestClass


class TestTrinoRemoteQueryExecutor(BaseTrinoTestClass, BaseRemoteQueryExecutorTestClass):
    SYNC_ADAPTER_CLS = TrinoDefaultAdapter
    ASYNC_ADAPTER_CLS = TrinoDefaultAdapter

    @pytest.mark.asyncio
    async def test_qe_result(self, remote_adapter: RemoteAsyncAdapter) -> None:
        result = await self.execute_request(remote_adapter, query=SUBSELECT_QUERY)
        assert len(result) == 3

    @pytest.mark.asyncio
    async def test_table_exists(self, remote_adapter: RemoteAsyncAdapter) -> None:
        table_ident = TableIdent(
            db_name="test_mysql_catalog", schema_name="test_data", table_name="table_ueopabpvy53rqgtrqqyeos"
        )
        result = await remote_adapter.is_table_exists(table_ident)
        assert result is True
