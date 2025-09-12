import attr
import pytest

from dl_app_tools.log.context import log_context
from dl_core.connection_executors.models.exc import QueryExecutorException
from dl_core_testing.testcases.remote_query_executor import BaseRemoteQueryExecutorTestClass
from dl_core_tests.db.base import DefaultCoreTestClass

from dl_connector_clickhouse.core.clickhouse.adapters import DLClickHouseAdapter


class TestRQE(DefaultCoreTestClass, BaseRemoteQueryExecutorTestClass):
    SYNC_ADAPTER_CLS = DLClickHouseAdapter
    ASYNC_ADAPTER_CLS = DLClickHouseAdapter

    @pytest.mark.asyncio
    async def test_body_signature_validation(self, remote_adapter, query_executor_options):
        # OK case
        result = await self.execute_request(remote_adapter, query="select 1")
        assert result[0][0] == 1

        # OK case, alternative secret key
        remote_adapter = attr.evolve(
            remote_adapter,
            rqe_data=attr.evolve(query_executor_options, hmac_key=self.EXT_QUERY_EXECUTER_SECRET_KEY_ALT.encode()),
        )
        result = await self.execute_request(remote_adapter, query="select 1")
        assert result[0][0] == 1

        # Not OK case
        remote_adapter = attr.evolve(
            remote_adapter,
            rqe_data=attr.evolve(query_executor_options, hmac_key=b"not_so_secret_key"),
        )
        async with remote_adapter:
            with pytest.raises(QueryExecutorException, match=r"Invalid signature"):
                await self.execute_request(remote_adapter, query="select 1")

    @staticmethod
    def _validate_logging_ctx(record, outer_logging_ctx):
        filtered_inner_ctx = {k: v for k, v in record.log_context.items() if k in outer_logging_ctx}
        assert filtered_inner_ctx == outer_logging_ctx

    @pytest.mark.asyncio
    async def test_qe_logging_ctx_propagation(self, remote_adapter, caplog):
        if not remote_adapter._force_async_rqe:
            pytest.skip("Find a way to check logs of subprocess")

        caplog.set_level("INFO")
        outer_logging_ctx = dict(some_str_key="some_val", some_int_key=123)

        with log_context(**outer_logging_ctx):
            result = await self.execute_request(remote_adapter, query="select 1")
            assert result[0][0] == 1

        qe_logs = list(
            filter(lambda r: r.name == "dl_core.connection_executors.remote_query_executor.app_async", caplog.records)
        )
        assert qe_logs
        for qe_record in qe_logs:
            self._validate_logging_ctx(qe_record, outer_logging_ctx)

        cursor_executed_logs = list(
            filter(lambda r: r.name == "dl_core.connection_executors.adapters.sa_utils", caplog.records)
        )
        assert len(cursor_executed_logs) == 1
        rec = cursor_executed_logs[0]
        self._validate_logging_ctx(rec, outer_logging_ctx)
