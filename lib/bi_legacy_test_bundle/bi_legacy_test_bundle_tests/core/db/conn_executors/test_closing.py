from __future__ import annotations

import pytest

from dl_api_commons.base_models import RequestContextInfo
from dl_connector_postgresql.core.postgresql.dto import PostgresConnDTO
from dl_connector_postgresql.core.postgresql_base.adapters_postgres import PostgresAdapter
from dl_connector_postgresql.core.postgresql_base.target_dto import PostgresConnTargetDTO
from dl_core.connection_executors.adapters.async_adapters_sync_wrapper import AsyncWrapperForSyncAdapter
from dl_core.connection_executors.models.db_adapter_data import DBAdapterQuery
from dl_core.connections_security.base import InsecureConnectionSecurityManager
from dl_core.services_registry.conn_executor_factory import DefaultConnExecutorFactory
from dl_core.services_registry.top_level import DummyServiceRegistry
from dl_core.us_manager.us_manager_dummy import DummyUSManager
from dl_core.utils import FutureRef
from dl_core_testing.connection import make_connection
from dl_utils.aio import ContextVarExecutor


class TestClosing:
    @pytest.fixture()
    def pg_conn_target_dto(self, postgres_db) -> PostgresConnTargetDTO:
        return PostgresConnTargetDTO(
            conn_id=None,
            host=postgres_db.url.host,
            port=postgres_db.url.port,
            db_name=postgres_db.url.database,
            username=postgres_db.url.username,
            password=postgres_db.url.password,
        )

    @pytest.fixture()
    def pg_conn(self, postgres_db, bi_context):
        return make_connection(us_manager=DummyUSManager(bi_context=bi_context), db=postgres_db)

    @pytest.fixture()
    def pg_conn_dto(self, postgres_db) -> PostgresConnDTO:
        return PostgresConnDTO(
            conn_id=None,
            host=postgres_db.url.host,
            port=postgres_db.url.port,
            db_name=postgres_db.url.database,
            username=postgres_db.url.username,
            password=postgres_db.url.password,
        )

    @pytest.fixture()
    def tpe(self):
        tpe = ContextVarExecutor()
        yield tpe
        tpe.shutdown()

    @pytest.mark.asyncio
    async def test_async_wrapper_closing(self, tpe, pg_conn_target_dto, caplog):
        caplog.set_level("INFO")
        dba = AsyncWrapperForSyncAdapter(
            sync_adapter=PostgresAdapter(
                target_dto=pg_conn_target_dto, default_chunk_size=1, req_ctx_info=RequestContextInfo.create_empty()
            ),
            tpe=tpe,
        )

        await dba.execute(DBAdapterQuery("SELECT 1"))
        await dba.close()
        assert tpe.get_pending_futures_count() == 0

    # TODO FIX: Move to tests.service_registry
    @pytest.mark.asyncio
    @pytest.mark.usefixtures("app_request_context")
    async def test_ce_factory_closing_async(self, pg_conn, bi_context, rqe_config_subprocess):
        f = DefaultConnExecutorFactory(
            async_env=True,
            services_registry_ref=FutureRef().fulfilled(DummyServiceRegistry(bi_context)),
            conn_sec_mgr=InsecureConnectionSecurityManager(),
            rqe_config=rqe_config_subprocess,
            tpe=None,
        )
        executor = f.get_async_conn_executor(pg_conn)
        await executor.initialize()

        # Ensure sync closing is prohibited
        with pytest.raises(NotImplementedError):
            f.close_sync()

        await f.close_async()

    # TODO FIX: Move to tests.service_registry
    @pytest.mark.usefixtures("app_request_context", "loop")
    def test_ce_factory_closing_sync(self, pg_conn, bi_context, rqe_config_subprocess):
        f = DefaultConnExecutorFactory(
            async_env=False,
            services_registry_ref=FutureRef().fulfilled(DummyServiceRegistry(bi_context)),
            conn_sec_mgr=InsecureConnectionSecurityManager(),
            rqe_config=rqe_config_subprocess,
            tpe=None,
        )
        executor = f.get_sync_conn_executor(pg_conn)
        executor.initialize()

        # Ensure sync closing is prohibited
        with pytest.raises(NotImplementedError):
            f.close_async()

        f.close_sync()
