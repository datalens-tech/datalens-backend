import secrets
from typing import (
    Generic,
    TypeVar,
)

import attr
import pytest

from dl_core.connection_executors.adapters.async_adapters_remote import RemoteAsyncAdapter
from dl_core.connection_executors.models.common import RemoteQueryExecutorData
from dl_core.connection_executors.models.connection_target_dto_base import ConnTargetDTO
from dl_core.connection_executors.models.db_adapter_data import DBAdapterQuery
from dl_core.connection_executors.models.exc import QueryExecutorException
from dl_core.connection_executors.models.scoped_rci import DBAdapterScopedRCI
from dl_core.us_connection_base import ConnectionBase
from dl_core_testing.testcases.remote_query_executor import BaseRemoteQueryExecutorTestClass
import dl_logging

_CONN_TV = TypeVar("_CONN_TV", bound=ConnectionBase)


class DefaultQESpecificTestSuite(BaseRemoteQueryExecutorTestClass[_CONN_TV], Generic[_CONN_TV]):
    """Suite for RQE body-signature validation and log-context propagation.

    Inherits the parent's `query_executor_options`, `conn_target_dto`,
    `SYNC_ADAPTER_CLS`, `ASYNC_ADAPTER_CLS`. Connector subclasses just declare
    those ClassVars and provide the standard connection fixtures.
    """

    @pytest.mark.asyncio
    @pytest.mark.parametrize("force_async_rqe", (True, False))
    async def test_body_signature_validation(
        self,
        conn_target_dto: ConnTargetDTO,
        query_executor_options: RemoteQueryExecutorData,
        force_async_rqe: bool,
    ) -> None:
        # OK case: matching HMAC.
        remote_adapter = RemoteAsyncAdapter(
            target_dto=conn_target_dto,
            dba_cls=self.SYNC_ADAPTER_CLS,
            rqe_data=query_executor_options,
            req_ctx_info=DBAdapterScopedRCI(),
            force_async_rqe=force_async_rqe,
        )
        async with remote_adapter:
            res = await remote_adapter.execute(DBAdapterQuery("select 1"))
            async for row in res.raw_chunk_generator:
                assert row

        # NOT OK case: mismatched HMAC.
        remote_adapter = RemoteAsyncAdapter(
            target_dto=conn_target_dto,
            dba_cls=self.SYNC_ADAPTER_CLS,
            rqe_data=attr.evolve(query_executor_options, hmac_key=secrets.token_bytes(128)),
            req_ctx_info=DBAdapterScopedRCI(),
            force_async_rqe=force_async_rqe,
        )
        async with remote_adapter:
            with pytest.raises(QueryExecutorException, match=r"Invalid signature"):
                await remote_adapter.execute(DBAdapterQuery("select 1"))

    @pytest.mark.asyncio
    @pytest.mark.parametrize("force_async_rqe", (True, False))
    async def test_qe_logging_ctx_propagation(
        self,
        query_executor_options: RemoteQueryExecutorData,
        conn_target_dto: ConnTargetDTO,
        caplog: pytest.LogCaptureFixture,
        force_async_rqe: bool,
    ) -> None:
        if not force_async_rqe:
            pytest.skip("Find a way to check logs of subprocess")

        caplog.set_level("INFO")
        remote_adapter = RemoteAsyncAdapter(
            target_dto=conn_target_dto,
            dba_cls=self.SYNC_ADAPTER_CLS,
            rqe_data=query_executor_options,
            req_ctx_info=DBAdapterScopedRCI(),
            force_async_rqe=force_async_rqe,
        )

        outer_logging_ctx = {
            "some_str_key": "some_val",
            "some_int_key": 123,
        }

        with dl_logging.LogContext(context=outer_logging_ctx):
            res = await remote_adapter.execute(DBAdapterQuery("select 1"))

        async for chunk in res.raw_chunk_generator:
            print(chunk)

        qe_logs = list(
            filter(
                lambda r: r.name == "dl_core.connection_executors.remote_query_executor.app_async",
                caplog.records,
            )
        )
        assert qe_logs

        for qe_r in qe_logs:
            filtered_inner_ctx = {k: v for k, v in getattr(qe_r, "log_context", {}).items() if k in outer_logging_ctx}
            assert filtered_inner_ctx == outer_logging_ctx

        cursor_executed_logs = list(
            filter(
                lambda r: r.name == "dl_core.connection_executors.adapters.sa_utils",
                caplog.records,
            )
        )
        assert len(cursor_executed_logs) == 1
        rec = cursor_executed_logs[0]
        filtered_inner_ctx = {k: v for k, v in getattr(rec, "log_context", {}).items() if k in outer_logging_ctx}
        assert filtered_inner_ctx == outer_logging_ctx
