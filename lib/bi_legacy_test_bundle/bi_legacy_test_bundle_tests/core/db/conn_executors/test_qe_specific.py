from __future__ import annotations

import secrets

import attr
import pytest

from dl_connector_postgresql.core.postgresql_base.adapters_postgres import PostgresAdapter
from dl_core.connection_executors.adapters.async_adapters_remote import RemoteAsyncAdapter
from dl_connector_postgresql.core.postgresql_base.target_dto import PostgresConnTargetDTO
from dl_core.connection_executors.models.db_adapter_data import DBAdapterQuery
from dl_core.connection_executors.models.exc import QueryExecutorException
from dl_api_commons.base_models import RequestContextInfo
from dl_app_tools.log.context import log_context


class TestsQESpecific:
    @pytest.fixture()
    def db(self, postgres_db):
        return postgres_db

    @pytest.fixture()
    def conn_target_dto(self, db):
        return PostgresConnTargetDTO(
            conn_id=None,
            host=db.url.host,
            port=db.url.port,
            db_name=db.url.database,
            username=db.url.username,
            password=db.url.password,
        )

    @pytest.fixture()
    def dba_cls(self):
        return PostgresAdapter

    @pytest.mark.asyncio
    @pytest.mark.parametrize("force_async_rqe", (True, False))
    async def test_body_signature_validation(
            self,
            conn_target_dto,
            query_executor_options,
            force_async_rqe,
    ):
        # OK case
        remote_adapter = RemoteAsyncAdapter(
            target_dto=conn_target_dto,
            dba_cls=PostgresAdapter,
            rqe_data=query_executor_options,
            req_ctx_info=RequestContextInfo.create_empty(),
            force_async_rqe=force_async_rqe,
        )

        async with remote_adapter:
            res = await remote_adapter.execute(DBAdapterQuery("select 1"))
            async for row in res.raw_chunk_generator:
                assert row

        # Not OK case
        remote_adapter = RemoteAsyncAdapter(
            target_dto=conn_target_dto,
            dba_cls=PostgresAdapter,
            rqe_data=attr.evolve(query_executor_options, hmac_key=secrets.token_bytes(128)),
            req_ctx_info=RequestContextInfo.create_empty(),
            force_async_rqe=force_async_rqe,
        )

        async with remote_adapter:
            with pytest.raises(QueryExecutorException, match=r'Invalid signature'):
                await remote_adapter.execute(DBAdapterQuery("select 1"))

    @pytest.mark.asyncio
    @pytest.mark.parametrize("force_async_rqe", (True, False))
    async def test_qe_logging_ctx_propagation(
            self,
            query_executor_options,
            db,
            conn_target_dto,
            caplog,
            force_async_rqe,
    ):
        if not force_async_rqe:
            pytest.skip("Find a way to check logs of subprocess")

        caplog.set_level('INFO')

        remote_adapter = RemoteAsyncAdapter(
            target_dto=conn_target_dto,
            dba_cls=PostgresAdapter,
            rqe_data=query_executor_options,
            req_ctx_info=RequestContextInfo.create_empty(),
            force_async_rqe=force_async_rqe,
        )

        outer_logging_ctx = {
            'some_str_key': 'some_val',
            'some_int_key': 123,
        }

        with log_context(**outer_logging_ctx):
            res = await remote_adapter.execute(DBAdapterQuery("select 1"))

        async for chunk in res.raw_chunk_generator:
            print(chunk)

        qe_logs = list(filter(
            lambda r: r.name == "dl_core.connection_executors.remote_query_executor.app_async",
            caplog.records
        ))
        assert qe_logs

        for qe_r in qe_logs:
            filtered_inner_ctx = {
                k: v
                for k, v in qe_r.log_context.items() if k in outer_logging_ctx
            }

            assert filtered_inner_ctx == outer_logging_ctx

        # TODO FIX: Move to dedicated test
        cursor_executed_logs = list(
            filter(lambda r: r.name == "dl_core.connection_executors.adapters.sa_utils", caplog.records)
        )

        assert len(cursor_executed_logs) == 1
        rec = cursor_executed_logs[0]
        filtered_inner_ctx = {
            k: v
            for k, v in rec.log_context.items() if k in outer_logging_ctx
        }
        assert filtered_inner_ctx == outer_logging_ctx
