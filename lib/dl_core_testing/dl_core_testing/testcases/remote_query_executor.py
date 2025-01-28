import asyncio
from typing import (
    AsyncGenerator,
    ClassVar,
    Generator,
    Generic,
    Type,
    TypeVar,
)

from aiohttp.pytest_plugin import AiohttpClient
from aiohttp.test_utils import TestClient
import pytest

from dl_configs.rqe import (
    RQEBaseURL,
    RQEExecuteRequestMode,
)
from dl_constants.types import TBIDataRow
from dl_core.connection_executors import DefaultSqlAlchemyConnExecutor
from dl_core.connection_executors.adapters.async_adapters_remote import RemoteAsyncAdapter
from dl_core.connection_executors.adapters.common_base import CommonBaseDirectAdapter
from dl_core.connection_executors.async_base import AsyncConnExecutorBase
from dl_core.connection_executors.models.common import RemoteQueryExecutorData
from dl_core.connection_executors.models.connection_target_dto_base import ConnTargetDTO
from dl_core.connection_executors.models.db_adapter_data import DBAdapterQuery
from dl_core.connection_executors.models.scoped_rci import DBAdapterScopedRCI
from dl_core.connection_executors.remote_query_executor.app_async import create_async_qe_app
from dl_core.exc import SourceTimeout
from dl_core.us_connection_base import ConnectionBase
from dl_core_testing.rqe import RQEConfigurationMaker
from dl_core_testing.testcases.connection_executor import BaseConnectionExecutorTestClass
from dl_utils.aio import alist


_CONN_TV = TypeVar("_CONN_TV", bound=ConnectionBase)


class BaseRemoteQueryExecutorTestClass(BaseConnectionExecutorTestClass[_CONN_TV], Generic[_CONN_TV]):
    SYNC_ADAPTER_CLS: ClassVar[Type[CommonBaseDirectAdapter]]
    ASYNC_ADAPTER_CLS: ClassVar[Type[CommonBaseDirectAdapter]]

    EXT_QUERY_EXECUTER_SECRET_KEY: ClassVar[str] = "very_secret_key"

    @pytest.fixture(scope="function")
    def forbid_private_addr(self) -> bool:
        return False

    @pytest.fixture(scope="class")
    def basic_test_query(self) -> str:
        return "select 1"

    @pytest.fixture(scope="function")
    def query_executor_app(
        self, loop: asyncio.AbstractEventLoop, aiohttp_client: AiohttpClient, forbid_private_addr: bool
    ) -> TestClient:
        app = create_async_qe_app(
            hmac_key=self.EXT_QUERY_EXECUTER_SECRET_KEY.encode(), forbid_private_addr=forbid_private_addr
        )
        return loop.run_until_complete(aiohttp_client(app))

    @pytest.fixture(scope="function")
    def sync_rqe_netloc_subprocess(self, forbid_private_addr: bool) -> Generator[RQEBaseURL, None, None]:
        with RQEConfigurationMaker(
            ext_query_executer_secret_key=self.EXT_QUERY_EXECUTER_SECRET_KEY,
            core_connector_whitelist=self.core_test_config.core_connector_ep_names,
            forbid_private_addr="1" if forbid_private_addr else "0",
        ).sync_rqe_netloc_subprocess_cm() as sync_rqe_config:
            yield sync_rqe_config

    @pytest.fixture(scope="function", params=[RQEExecuteRequestMode.STREAM, RQEExecuteRequestMode.NON_STREAM])
    def query_executor_options(
        self,
        query_executor_app: TestClient,
        sync_rqe_netloc_subprocess: RQEBaseURL,
        request: pytest.FixtureRequest,
    ) -> RemoteQueryExecutorData:
        assert query_executor_app.port is not None
        return RemoteQueryExecutorData(
            hmac_key=self.EXT_QUERY_EXECUTER_SECRET_KEY.encode(),
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
    async def conn_target_dto(
        self,
        async_connection_executor: AsyncConnExecutorBase,
    ) -> AsyncGenerator[ConnTargetDTO, None]:
        assert isinstance(async_connection_executor, DefaultSqlAlchemyConnExecutor)
        target_conn_dto_pool = await async_connection_executor._make_target_conn_dto_pool()
        yield next(iter(target_conn_dto_pool))

    @pytest.fixture(scope="function", params=[True, False], ids=["async", "sync"])
    async def remote_adapter(
        self,
        conn_target_dto: ConnTargetDTO,
        query_executor_options: RemoteQueryExecutorData,
        request: pytest.FixtureRequest,
    ) -> RemoteAsyncAdapter:
        return RemoteAsyncAdapter(
            target_dto=conn_target_dto,
            dba_cls=self.ASYNC_ADAPTER_CLS if request.param else self.SYNC_ADAPTER_CLS,
            rqe_data=query_executor_options,
            req_ctx_info=DBAdapterScopedRCI(),
            force_async_rqe=request.param,
        )

    @pytest.fixture(scope="function", params=[True, False], ids=["json", "pickle"], autouse=True)
    def use_new_qe_serializer(self, request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:
        if request.param:
            monkeypatch.setenv("USE_NEW_QE_SERIALIZER", "1")

    async def execute_request(self, remote_adapter: RemoteAsyncAdapter, query: str) -> list[TBIDataRow]:
        async with remote_adapter:
            resp = await remote_adapter.execute(DBAdapterQuery(query))
            result = await alist(resp.get_all_rows())
        return result

    @pytest.mark.parametrize("forbid_private_addr", [True])
    async def test_forbid_private_hosts(
        self, remote_adapter: RemoteAsyncAdapter, forbid_private_addr: bool, basic_test_query: str
    ) -> None:
        async with remote_adapter:
            with pytest.raises(SourceTimeout):
                await self.execute_request(remote_adapter, query=basic_test_query)
