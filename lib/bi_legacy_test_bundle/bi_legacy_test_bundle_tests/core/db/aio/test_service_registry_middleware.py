from __future__ import annotations

from aiohttp import web
import pytest
import redis.asyncio

from dl_api_commons.aio.middlewares.commit_rci import commit_rci_middleware
from dl_api_commons.aio.middlewares.request_bootstrap import RequestBootstrap
from dl_api_commons.aio.middlewares.request_id import RequestId
from dl_api_commons.aiohttp.aiohttp_wrappers import RequiredResourceCommon
from dl_api_commons.base_models import RequestContextInfo
from dl_compeng_pg.compeng_asyncpg.pool_asyncpg import AsyncpgPoolWrapper
from dl_compeng_pg.compeng_asyncpg.processor_asyncpg import AsyncpgOperationProcessor
from dl_compeng_pg.compeng_pg_base.data_processor_service_pg import CompEngPgConfig
from dl_constants.enums import (
    ProcessorType,
    RedisInstanceKind,
)
from dl_core.aio.aiohttp_wrappers_data_core import (
    DLRequestDataCore,
    DLRequestDataCoreView,
)
from dl_core.aio.middlewares.services_registry import services_registry_middleware
from dl_core.aio.middlewares.us_manager import service_us_manager_middleware
from dl_core.aio.web_app_services.data_processing.factory import make_compeng_service
from dl_core.aio.web_app_services.redis import SingleHostSimpleRedisService
from dl_core.connections_security.base import (
    ConnectionSecurityManager,
    InsecureConnectionSecurityManager,
)
from dl_core.mdb_utils import MDBDomainManagerSettings
from dl_core.services_registry.env_manager_factory import DefaultEnvManagerFactory
from dl_core.services_registry.sr_factories import DefaultSRFactory
from dl_core.us_dataset import Dataset

# TODO?: move to unit tests (with fake `qc_redis_url`)


@pytest.mark.asyncio
async def test_integration_conn_executor_factory(
    aiohttp_client, rqe_config_subprocess, saved_pg_connection, caplog, core_test_config
):
    caplog.set_level("INFO")

    class CustomTestEnvManagerFactory(DefaultEnvManagerFactory):
        def make_security_manager(self, request_context_info: RequestContextInfo) -> ConnectionSecurityManager:
            return InsecureConnectionSecurityManager()

    us_config = core_test_config.get_us_config()
    app = web.Application(
        middlewares=[
            RequestBootstrap(
                req_id_service=RequestId(dl_request_cls=DLRequestDataCore),
            ).middleware,
            commit_rci_middleware(),
            services_registry_middleware(
                DefaultSRFactory(
                    async_env=True,
                    rqe_config=rqe_config_subprocess,
                    env_manager_factory=CustomTestEnvManagerFactory(),
                    mdb_domain_manager_settings=MDBDomainManagerSettings(),
                )
            ),
            service_us_manager_middleware(
                us_base_url=us_config.us_host,
                us_master_token=us_config.us_master_token,
                crypto_keys_config=core_test_config.get_crypto_keys_config(),
            ),
        ]
    )

    class TestView(DLRequestDataCoreView):
        REQUIRED_RESOURCES = frozenset({RequiredResourceCommon.SERVICE_US_MANAGER})

        async def get(self):
            ce_factory = self.dl_request.services_registry.get_conn_executor_factory()
            conn = await self.dl_request.service_us_manager.get_by_id(saved_pg_connection.uuid)
            ce = ce_factory.get_async_conn_executor(conn)

            await ce.test()
            return web.json_response({})

    app.router.add_route("*", "/simple_view", TestView)

    client = await aiohttp_client(app)
    resp = await client.get("/simple_view")
    try:
        assert resp.status == 200
    finally:
        resp.close()


@pytest.mark.asyncio
async def test_integration_redis_not_configured_case(aiohttp_client, rqe_config_subprocess, caplog):
    caplog.set_level("INFO")

    class CustomTestEnvManagerFactory(DefaultEnvManagerFactory):
        def make_security_manager(self, request_context_info: RequestContextInfo) -> ConnectionSecurityManager:
            return InsecureConnectionSecurityManager()

    app = web.Application(
        middlewares=[
            RequestBootstrap(
                req_id_service=RequestId(dl_request_cls=DLRequestDataCore),
            ).middleware,
            commit_rci_middleware(),
            services_registry_middleware(
                DefaultSRFactory(
                    async_env=True,
                    rqe_config=rqe_config_subprocess,
                    env_manager_factory=CustomTestEnvManagerFactory(),
                    mdb_domain_manager_settings=MDBDomainManagerSettings(),
                ),
                use_query_cache=False,
            ),
        ]
    )

    class TestView(DLRequestDataCoreView):
        async def get(self):
            assert self.dl_request.services_registry.get_caches_redis_client() is None
            return web.json_response({})

    app.router.add_route("*", "/simple_view", TestView)
    client = await aiohttp_client(app)
    async with client.get("/simple_view") as resp:
        assert resp.status == 200


@pytest.mark.asyncio
async def test_integration_redis_configured_case(aiohttp_client, rqe_config_subprocess, qc_redis_url, caplog):
    caplog.set_level("INFO")

    class CustomTestEnvManagerFactory(DefaultEnvManagerFactory):
        def make_security_manager(self, request_context_info: RequestContextInfo) -> ConnectionSecurityManager:
            return InsecureConnectionSecurityManager()

    app = web.Application(
        middlewares=[
            RequestBootstrap(
                req_id_service=RequestId(dl_request_cls=DLRequestDataCore),
            ).middleware,
            commit_rci_middleware(),
            services_registry_middleware(
                DefaultSRFactory(
                    async_env=True,
                    rqe_config=rqe_config_subprocess,
                    env_manager_factory=CustomTestEnvManagerFactory(),
                    mdb_domain_manager_settings=MDBDomainManagerSettings(),
                )
            ),
        ]
    )

    redis_service = SingleHostSimpleRedisService(
        instance_kind=RedisInstanceKind.caches,
        url=qc_redis_url,
    )
    app.on_startup.append(redis_service.init_hook)
    app.on_cleanup.append(redis_service.tear_down_hook)

    class TestView(DLRequestDataCoreView):
        async def get(self):
            assert isinstance(self.dl_request.services_registry.get_caches_redis_client(), redis.asyncio.Redis)
            return web.json_response({})

    app.router.add_route("*", "/simple_view", TestView)
    client = await aiohttp_client(app)
    async with client.get("/simple_view") as resp:
        assert resp.status == 200


@pytest.mark.asyncio
async def test_integration_compeng_configured_case(
    aiohttp_client,
    rqe_config_subprocess,
    compeng_pg_dsn,
    compeng_type,
    caplog,
    core_test_config,
    saved_ch_dataset,
):
    dataset_id = saved_ch_dataset.uuid
    caplog.set_level("INFO")

    class CustomTestEnvManagerFactory(DefaultEnvManagerFactory):
        def make_security_manager(self, request_context_info: RequestContextInfo) -> ConnectionSecurityManager:
            return InsecureConnectionSecurityManager()

    us_config = core_test_config.get_us_config()
    app = web.Application(
        middlewares=[
            RequestBootstrap(
                req_id_service=RequestId(dl_request_cls=DLRequestDataCore),
            ).middleware,
            commit_rci_middleware(),
            services_registry_middleware(
                services_registry_factory=DefaultSRFactory(
                    async_env=True,
                    rqe_config=rqe_config_subprocess,
                    env_manager_factory=CustomTestEnvManagerFactory(),
                    mdb_domain_manager_settings=MDBDomainManagerSettings(),
                ),
            ),
            service_us_manager_middleware(
                us_base_url=us_config.us_host,
                us_master_token=us_config.us_master_token,
                crypto_keys_config=core_test_config.get_crypto_keys_config(),
            ),
        ]
    )

    compeng_pg_service = make_compeng_service(
        processor_type=compeng_type,
        config=CompEngPgConfig(url=compeng_pg_dsn),
    )

    app.on_startup.append(compeng_pg_service.init_hook)
    app.on_cleanup.append(compeng_pg_service.tear_down_hook)

    class TestView(DLRequestDataCoreView):
        REQUIRED_RESOURCES = frozenset({RequiredResourceCommon.SERVICE_US_MANAGER})

        async def get(self):
            service_us_manager = self.dl_request.service_us_manager
            dataset = await service_us_manager.get_by_id(dataset_id, Dataset)
            processor_factory = self.dl_request.services_registry.get_data_processor_factory()
            processor = await processor_factory.get_data_processor(
                dataset=dataset,
                processor_type=ProcessorType.ASYNCPG,
                allow_cache_usage=False,
                us_entry_buffer=service_us_manager.get_entry_buffer(),
            )
            assert isinstance(processor, AsyncpgOperationProcessor)
            assert isinstance(processor._pg_pool, AsyncpgPoolWrapper)  # noqa
            return web.json_response({})

    app.router.add_route("*", "/simple_view", TestView)
    client = await aiohttp_client(app)
    async with client.get("/simple_view") as resp:
        assert resp.status == 200
