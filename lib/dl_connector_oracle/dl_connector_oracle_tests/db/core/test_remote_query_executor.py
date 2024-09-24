import pytest

from dl_core.exc import SourceTimeout
from dl_core_testing.testcases.remote_query_executor import BaseRemoteQueryExecutorTestClass

from dl_connector_oracle.core.adapters_oracle import OracleDefaultAdapter
from dl_connector_oracle_tests.db.config import SUBSELECT_QUERY_FULL
from dl_connector_oracle_tests.db.core.base import BaseOracleTestClass


class TestOracleRemoteQueryExecutor(BaseOracleTestClass, BaseRemoteQueryExecutorTestClass):
    SYNC_ADAPTER_CLS = OracleDefaultAdapter
    ASYNC_ADAPTER_CLS = OracleDefaultAdapter

    @pytest.mark.asyncio
    async def test_qe_result(self, remote_adapter):
        result = await self.execute_request(remote_adapter, query=SUBSELECT_QUERY_FULL)
        assert len(result) == 3

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
