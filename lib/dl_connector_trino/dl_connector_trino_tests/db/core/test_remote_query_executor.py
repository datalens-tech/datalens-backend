import pytest

from dl_core_testing.testcases.remote_query_executor import BaseRemoteQueryExecutorTestClass

from dl_connector_trino.core.adapters import TrinoDefaultAdapter
from dl_connector_trino_tests.db.config import SUBSELECT_QUERY_FULL
from dl_connector_trino_tests.db.core.base import BaseTrinoTestClass


class TestTrinoRemoteQueryExecutor(BaseTrinoTestClass, BaseRemoteQueryExecutorTestClass):
    SYNC_ADAPTER_CLS = TrinoDefaultAdapter
    ASYNC_ADAPTER_CLS = TrinoDefaultAdapter

    @pytest.mark.asyncio
    async def test_qe_result(self, remote_adapter):
        result = await self.execute_request(remote_adapter, query=SUBSELECT_QUERY_FULL)
        assert len(result) == 3
