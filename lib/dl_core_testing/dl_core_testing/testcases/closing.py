import asyncio
from typing import (
    AsyncGenerator,
    ClassVar,
    Generator,
    Generic,
    TypeVar,
)

import pytest
import pytest_asyncio

from dl_api_commons.base_models import RequestContextInfo
from dl_configs.rqe import RQEConfig
from dl_core.connection_executors.adapters.adapters_base import SyncDirectDBAdapter
from dl_core.connection_executors.adapters.async_adapters_sync_wrapper import AsyncWrapperForSyncAdapter
from dl_core.connection_executors.async_base import AsyncConnExecutorBase
from dl_core.connection_executors.models.connection_target_dto_base import ConnTargetDTO
from dl_core.connection_executors.models.db_adapter_data import DBAdapterQuery
from dl_core.connection_executors.models.scoped_rci import DBAdapterScopedRCI
from dl_core.connections_security.base import InsecureConnectionSecurityManager
from dl_core.services_registry.conn_executor_factory import DefaultConnExecutorFactory
from dl_core.services_registry.top_level import (
    DummyServiceRegistry,
    ServicesRegistry,
)
from dl_core.us_connection_base import ConnectionBase
from dl_core.utils import FutureRef
from dl_core_testing.testcases.connection_executor import BaseConnectionExecutorTestClass
from dl_utils.aio import ContextVarExecutor

_CONN_TV = TypeVar("_CONN_TV", bound=ConnectionBase)


class DefaultClosingTestSuite(BaseConnectionExecutorTestClass[_CONN_TV], Generic[_CONN_TV]):
    """Suite for `AsyncWrapperForSyncAdapter` and `DefaultConnExecutorFactory` closing.

    Connector subclasses must declare:
    - `SYNC_ADAPTER_CLS`: the connector's direct sync adapter class.
    """

    SYNC_ADAPTER_CLS: ClassVar[type[SyncDirectDBAdapter]]

    @pytest_asyncio.fixture(scope="function")
    async def conn_target_dto(
        self,
        async_connection_executor: AsyncConnExecutorBase,
    ) -> AsyncGenerator[ConnTargetDTO, None]:
        from dl_core.connection_executors import DefaultSqlAlchemyConnExecutor

        assert isinstance(async_connection_executor, DefaultSqlAlchemyConnExecutor)
        target_conn_dto_pool = await async_connection_executor._make_target_conn_dto_pool()
        yield next(iter(target_conn_dto_pool))

    @pytest.fixture(scope="function")
    def tpe(self) -> Generator[ContextVarExecutor, None, None]:
        tpe = ContextVarExecutor()
        yield tpe
        tpe.shutdown()

    @pytest.mark.asyncio
    async def test_async_wrapper_closing(
        self,
        tpe: ContextVarExecutor,
        conn_target_dto: ConnTargetDTO,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        caplog.set_level("INFO")
        dba = AsyncWrapperForSyncAdapter(
            sync_adapter=self.SYNC_ADAPTER_CLS.create(
                target_dto=conn_target_dto,
                default_chunk_size=1,
                req_ctx_info=DBAdapterScopedRCI(),
            ),
            tpe=tpe,
        )
        await dba.execute(DBAdapterQuery("SELECT 1"))
        await dba.close()
        assert tpe.get_pending_futures_count() == 0

    @pytest.mark.asyncio
    async def test_ce_factory_closing_async(
        self,
        saved_connection: _CONN_TV,
        conn_bi_context: RequestContextInfo,
        root_certificates: bytes,
    ) -> None:
        f = DefaultConnExecutorFactory(
            async_env=True,
            services_registry_ref=FutureRef[ServicesRegistry]().fulfilled(DummyServiceRegistry(conn_bi_context)),
            conn_sec_mgr=InsecureConnectionSecurityManager(),
            rqe_config=RQEConfig.get_default(),
            tpe=None,
            ca_data=root_certificates,
        )
        executor = f.get_async_conn_executor(saved_connection)
        await executor.initialize()

        with pytest.raises(NotImplementedError):
            f.close_sync()

        await f.close_async()

    def test_ce_factory_closing_sync(
        self,
        saved_connection: _CONN_TV,
        conn_bi_context: RequestContextInfo,
        root_certificates: bytes,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        f = DefaultConnExecutorFactory(
            async_env=False,
            services_registry_ref=FutureRef[ServicesRegistry]().fulfilled(DummyServiceRegistry(conn_bi_context)),
            conn_sec_mgr=InsecureConnectionSecurityManager(),
            rqe_config=RQEConfig.get_default(),
            tpe=None,
            ca_data=root_certificates,
        )
        executor = f.get_sync_conn_executor(saved_connection)
        executor.initialize()

        with pytest.raises(NotImplementedError):
            # The factory decorator raises sync before the async body runs;
            # binding the result avoids mypy's `unused-coroutine` flag.
            _ = f.close_async()

        f.close_sync()
