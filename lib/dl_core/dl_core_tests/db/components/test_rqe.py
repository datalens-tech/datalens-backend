import attr
import pytest

from dl_app_tools.log.context import log_context
from dl_configs.rqe import RQEExecuteRequestMode
from dl_core.connection_executors.adapters.async_adapters_remote import RemoteAsyncAdapter
from dl_core.connection_executors.models.common import RemoteQueryExecutorData
from dl_core.connection_executors.models.db_adapter_data import DBAdapterQuery
from dl_core.connection_executors.models.exc import QueryExecutorException
from dl_core.connection_executors.models.scoped_rci import DBAdapterScopedRCI
from dl_core.connection_executors.remote_query_executor.app_async import create_async_qe_app
from dl_core_testing.rqe import RQEConfigurationMaker
from dl_core_tests.db.base import DefaultCoreTestClass

from dl_connector_clickhouse.core.clickhouse_base.adapters import ClickHouseAdapter


EXT_QUERY_EXECUTER_SECRET_KEY = "very_secret_key"


class TestRQE(DefaultCoreTestClass):
    @pytest.fixture(scope="function")
    def query_executor_app(self, loop, aiohttp_client):
        app = create_async_qe_app(hmac_key=EXT_QUERY_EXECUTER_SECRET_KEY.encode())
        return loop.run_until_complete(aiohttp_client(app))

    @pytest.fixture(scope="function")
    def sync_rqe_netloc_subprocess(self):
        with RQEConfigurationMaker(
            ext_query_executer_secret_key=EXT_QUERY_EXECUTER_SECRET_KEY,
            core_connector_whitelist=self.core_test_config.core_connector_ep_names,
        ).sync_rqe_netloc_subprocess_cm() as sync_rqe_config:
            yield sync_rqe_config

    @pytest.fixture(scope="function", params=[RQEExecuteRequestMode.STREAM, RQEExecuteRequestMode.NON_STREAM])
    def query_executor_options(self, query_executor_app, sync_rqe_netloc_subprocess, request):
        return RemoteQueryExecutorData(
            hmac_key=EXT_QUERY_EXECUTER_SECRET_KEY.encode(),
            # Async RQE
            async_protocol="http",
            async_host=query_executor_app.host,
            async_port=query_executor_app.port,
            # Sync RQE
            sync_protocol="http",
            sync_host=sync_rqe_netloc_subprocess.host,
            sync_port=sync_rqe_netloc_subprocess.port,
            execute_request_mode=request.param,
        )

    @pytest.fixture(scope="function")
    async def conn_target_dto(self, async_conn_executor_factory):
        sync_conn_executor = async_conn_executor_factory()
        target_conn_dto_pool = await sync_conn_executor._make_target_conn_dto_pool()
        yield next(iter(target_conn_dto_pool))
        await sync_conn_executor.close()

    @pytest.fixture(scope="function", params=[True, False])
    def remote_adapter(self, conn_target_dto, query_executor_options, request):
        return RemoteAsyncAdapter(
            target_dto=conn_target_dto,
            dba_cls=ClickHouseAdapter,
            rqe_data=query_executor_options,
            req_ctx_info=DBAdapterScopedRCI.create_empty(),
            force_async_rqe=request.param,
        )

    @pytest.mark.asyncio
    async def test_body_signature_validation(self, remote_adapter, query_executor_options):
        # OK case
        async with remote_adapter:
            res = await remote_adapter.execute(DBAdapterQuery("select 1"))
            async for row in res.raw_chunk_generator:
                assert row

        # Not OK case
        remote_adapter = attr.evolve(
            remote_adapter,
            rqe_data=attr.evolve(query_executor_options, hmac_key=b"not_so_secret_key"),
        )
        async with remote_adapter:
            with pytest.raises(QueryExecutorException, match=r"Invalid signature"):
                await remote_adapter.execute(DBAdapterQuery("select 1"))

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
            res = await remote_adapter.execute(DBAdapterQuery("select 1"))
        async for row in res.raw_chunk_generator:
            assert row

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
