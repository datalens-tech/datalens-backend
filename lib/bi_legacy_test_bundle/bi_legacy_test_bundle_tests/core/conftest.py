from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Callable

import aiohttp.pytest_plugin
from flask import (
    Flask,
    current_app,
)
import pytest
import redis.asyncio
from statcommons.logs import LOGMUTATORS

import bi_legacy_test_bundle_tests.core.config as tests_config_mod
from dl_api_commons.base_models import (
    RequestContextInfo,
    TenantCommon,
)
from dl_api_commons.flask.middlewares.context_var_middleware import ContextVarMiddleware
from dl_api_commons.reporting.registry import DefaultReportingRegistry
from dl_app_tools import log
from dl_compeng_pg.compeng_pg_base.data_processor_service_pg import CompEngPgConfig
from dl_configs import env_var_definitions
from dl_configs.rqe import (
    RQEBaseURL,
    RQEConfig,
)
from dl_connector_bundle_chs3.chs3_base.core.settings import FileS3ConnectorSettings
from dl_connector_bundle_chs3.chs3_gsheets.core.constants import CONNECTION_TYPE_GSHEETS_V2
from dl_connector_bundle_chs3.file.core.constants import CONNECTION_TYPE_FILE
from dl_connector_clickhouse.core.clickhouse_base.constants import CONNECTION_TYPE_CLICKHOUSE
from dl_constants.enums import ProcessorType
from dl_core.aio.web_app_services.data_processing.factory import make_compeng_service
from dl_core.connections_security.base import InsecureConnectionSecurityManager
from dl_core.loader import (
    CoreLibraryConfig,
    load_core_lib,
)
from dl_core.logging_config import add_log_context_scoped
from dl_core.mdb_utils import (
    MDBDomainManagerFactory,
    MDBDomainManagerSettings,
)
from dl_core.services_registry import (
    DefaultServicesRegistry,
    ServicesRegistry,
)
from dl_core.services_registry.cache_engine_factory import DefaultCacheEngineFactory
from dl_core.services_registry.conn_executor_factory import DefaultConnExecutorFactory
from dl_core.united_storage_client import USAuthContextMaster
from dl_core.us_manager.us_manager_async import AsyncUSManager
from dl_core.us_manager.us_manager_sync import SyncUSManager
from dl_core.utils import FutureRef
from dl_core_testing.configuration import CoreTestEnvironmentConfigurationBase
from dl_core_testing.database import (
    CoreDbDispenser,
    CoreReInitableDbDispenser,
    Db,
    make_db_config,
)
from dl_core_testing.environment import (
    common_pytest_configure,
    restart_container_by_label,
)
from dl_core_testing.fixture_server_runner import WSGIRunner
from dl_core_testing.utils import SROptions
from dl_db_testing.loader import load_db_testing_lib
from dl_task_processor.processor import DummyTaskProcessorFactory
from dl_testing.utils import wait_for_initdb

from bi_connector_mssql.core.constants import CONNECTION_TYPE_MSSQL


LOGGER = logging.getLogger(__name__)


# https://wiki.yandex-team.ru/users/riskingh/arcadiagotchas/#m-3.kakispolzovatpytest-aiohttp

# pytestmark = pytest.mark.asyncio  # for subpackages

pytest_plugins = (
    # 'pytest_asyncio.plugin',
    "aiohttp.pytest_plugin",
    "bi_testing_ya.pytest_plugin",
)
try:
    del aiohttp.pytest_plugin.loop
except AttributeError:
    pass


EXT_TEST_BLACKBOX_NAME = "Test"


@pytest.fixture(scope="session", autouse=True)
def loaded_libraries(core_test_config: CoreTestEnvironmentConfigurationBase) -> None:
    load_db_testing_lib()
    load_core_lib(core_lib_config=core_test_config.get_core_library_config())


@pytest.fixture
def loop(event_loop):
    asyncio.set_event_loop(event_loop)
    yield event_loop
    # Attept to cover an old version of pytest-asyncio:
    # https://github.com/pytest-dev/pytest-asyncio/commit/51d986cec83fdbc14fa08015424c79397afc7ad9
    asyncio.set_event_loop_policy(None)


def pytest_configure(config):  # noqa
    config.addinivalue_line("markers", "slow: ...")
    config.addinivalue_line("markers", "yt: ...")

    # Add Log context to logging records (not only in format phase)
    LOGMUTATORS.apply(require=False)
    # TODO FIX: Replace with add_log_context after all tests will be refactored to use unscoped log context
    LOGMUTATORS.add_mutator("log_context_scoped", add_log_context_scoped)

    common_pytest_configure(
        use_jaeger_tracer=env_var_definitions.use_jaeger_tracer(),
        tracing_service_name="tests_bi_core",
    )


@pytest.fixture(scope="session")
def db_dispenser() -> CoreDbDispenser:
    db_dispenser = CoreReInitableDbDispenser()
    db_dispenser.add_reinit_hook(
        db_config=make_db_config(
            conn_type=CONNECTION_TYPE_MSSQL,
            url=tests_config_mod.DB_CONFIGURATIONS["mssql"][0],
        ),
        reinit_hook=lambda: restart_container_by_label(
            label=tests_config_mod.MSSQL_CONTAINER_LABEL,
            compose_project=tests_config_mod.COMPOSE_PROJECT_NAME,
        ),
    )
    return db_dispenser


@pytest.fixture(scope="function", autouse=True)
def clear_logging_context():
    log.context.reset_context()


@pytest.fixture(scope="session")
def tests_config():
    """Environment-configuration fixture"""
    # The module, but can easily be replaced with a class with properties.
    return tests_config_mod


@pytest.fixture(scope="session")
def bi_config(tests_config):  # TODO: remove in favor of `tests_config`
    return tests_config


@pytest.fixture(scope="session")
def core_test_config(tests_config):
    return tests_config.CORE_TEST_CONFIG


@pytest.fixture(scope="session")
def bi_context() -> RequestContextInfo:
    return RequestContextInfo.create(
        request_id=None,
        tenant=TenantCommon(),
        user_id=None,
        user_name=None,
        x_dl_debug_mode=False,
        endpoint_code=None,
        x_dl_context=None,
        plain_headers={},
        secret_headers={},
        auth_data=None,
    )


@pytest.fixture(scope="session")
def app_context(request, tests_config):
    app = Flask(__name__)
    ContextVarMiddleware().wrap_flask_app(app)

    with app.app_context() as ctx:
        yield ctx


@pytest.fixture(scope="function")
def app_request_context(app_context, request, tests_config):
    with current_app.test_request_context() as ctx:
        yield ctx


@pytest.fixture(scope="session")
def qc_redis_url(tests_config):
    return tests_config.QUERY_CACHE_REDIS_URL


@pytest.fixture(scope="session")
def mutation_redis_url(tests_config):
    return tests_config.MUTATION_CACHE_REDIS_URL


@pytest.fixture(scope="session")
def compeng_pg_dsn(tests_config) -> str:
    url = tests_config.DB_CONFIGURATIONS["postgres_fresh"][0]
    url = url.replace("bi_postgresql://", "postgresql://")
    return url


@pytest.fixture(scope="function")
async def qc_async_redis(loop, qc_redis_url):
    """
    aioredis client fixture.

    Can only work properly in `db` tests, but should probably be okay to
    instantiate without it.
    """
    # Note: `aioredis.create_redis(qc_redis_url)` returns a single-connection
    # version, which is not sufficient to subscribe and send commands at the
    # same time.
    rc = redis.asyncio.Redis.from_url(qc_redis_url)
    yield rc
    await rc.close()
    await rc.connection_pool.disconnect()


@pytest.fixture(scope="function")
async def mutation_async_redis(loop, mutation_redis_url):
    """
    aioredis client fixture.

    Can only work properly in `db` tests, but should probably be okay to
    instantiate without it.
    """
    # Note: `aioredis.create_redis(qc_redis_url)` returns a single-connection
    # version, which is not sufficient to subscribe and send commands at the
    # same time.
    rc = redis.asyncio.Redis.from_url(mutation_redis_url)
    yield rc
    await rc.close()
    await rc.connection_pool.disconnect()


@pytest.fixture(scope="session")
def compeng_type() -> ProcessorType:
    return ProcessorType.ASYNCPG


@pytest.fixture(scope="function")
async def pg_compeng_data_processor(loop, compeng_pg_dsn, compeng_type):
    """
    PG CompEng pool fixture.

    Can only work properly in `db` tests, but should probably be okay to
    instantiate without it.
    """
    processor = make_compeng_service(
        processor_type=compeng_type,
        config=CompEngPgConfig(url=compeng_pg_dsn),
    )
    await processor.initialize()
    try:
        yield processor
    finally:
        await processor.finalize()


@contextlib.contextmanager
def make_sync_rqe_netloc_subprocess(tests_config) -> RQEBaseURL:
    assert tests_config.EXT_QUERY_EXECUTER_SECRET_KEY, "must be nonempty"
    runner_cm = WSGIRunner(
        module="dl_core.bin.query_executor_sync",
        callable="app",
        ping_path="/ping",
        env=dict(
            # Ensure the key matches:
            EXT_QUERY_EXECUTER_SECRET_KEY=tests_config.EXT_QUERY_EXECUTER_SECRET_KEY,
            DEV_LOGGING="1",
        ),
    )
    with runner_cm as runner:
        yield RQEBaseURL(host=runner.bind_addr, port=runner.bind_port)


@pytest.fixture(scope="session")
def sync_rqe_netloc_subprocess(tests_config) -> RQEBaseURL:
    with make_sync_rqe_netloc_subprocess(tests_config) as result:
        yield result


# TODO FIX: Implement async RQE in subprocess
@pytest.fixture(scope="session")
def async_rqe_netloc_subprocess() -> RQEBaseURL:
    # Temp kostyl
    return RQEBaseURL(
        host="127.0.0.1",
        port=65500,
    )


@pytest.fixture(scope="session")
def rqe_config_subprocess(sync_rqe_netloc_subprocess, async_rqe_netloc_subprocess, tests_config) -> RQEConfig:
    return RQEConfig(
        hmac_key=tests_config.EXT_QUERY_EXECUTER_SECRET_KEY.encode(),
        ext_sync_rqe=sync_rqe_netloc_subprocess,
        ext_async_rqe=async_rqe_netloc_subprocess,
        int_sync_rqe=sync_rqe_netloc_subprocess,
        int_async_rqe=async_rqe_netloc_subprocess,
    )


@pytest.fixture(scope="session")
def clickhouse_db(db_dispenser) -> Db:
    url, cluster = tests_config_mod.DB_CONFIGURATIONS[CONNECTION_TYPE_CLICKHOUSE.name]
    return db_dispenser.get_database(
        db_config=make_db_config(url=url, conn_type=CONNECTION_TYPE_CLICKHOUSE, cluster=cluster),
    )


@pytest.fixture(scope="session")
def connectors_settings(clickhouse_db):
    ch_creds = clickhouse_db.get_conn_credentials(full=True)
    base_settings_params = dict(
        SECURE=False,
        HOST=ch_creds["host"],
        PORT=ch_creds["port"],
        USERNAME=ch_creds["username"],
        PASSWORD=ch_creds["password"],
    )
    files3_settings = dict(
        ACCESS_KEY_ID="accessKey1",
        SECRET_ACCESS_KEY="verySecretKey1",
        BUCKET="bi-file-uploader",
        S3_ENDPOINT="http://s3-storage:8000",
        **base_settings_params,
    )

    return {
        CONNECTION_TYPE_FILE: FileS3ConnectorSettings(**files3_settings),
        CONNECTION_TYPE_GSHEETS_V2: FileS3ConnectorSettings(**files3_settings),
    }


@pytest.fixture(scope="function")
def async_service_registry_factory(
    loop,
    rqe_config_subprocess,
    qc_async_redis,
    pg_compeng_data_processor,
    connectors_settings,
) -> Callable[[SROptions], ServicesRegistry]:
    created_service_registries = set()

    def factory(opts: SROptions):
        sr_future_ref: FutureRef[DefaultServicesRegistry] = FutureRef()
        reporting_registry = DefaultReportingRegistry(rci=opts.rci)
        new_sr = DefaultServicesRegistry(
            rci=opts.rci,
            conn_exec_factory=DefaultConnExecutorFactory(
                async_env=True,
                services_registry_ref=sr_future_ref,
                rqe_config=rqe_config_subprocess,
                conn_sec_mgr=InsecureConnectionSecurityManager(),
                mdb_mgr=MDBDomainManagerFactory().get_manager(),
                tpe=None,
            ),
            caches_redis_client_factory=(lambda _: qc_async_redis) if opts.with_caches else None,
            mutations_redis_client_factory=None,
            mutations_cache_factory=None,
            data_processor_service_factory=(lambda _: pg_compeng_data_processor) if opts.with_compeng_pg else None,
            connectors_settings=connectors_settings,
            cache_engine_factory=DefaultCacheEngineFactory(
                services_registry_ref=sr_future_ref,
                cache_save_background=opts.cache_save_background,
            ),
            reporting_registry=reporting_registry,
            mdb_domain_manager_factory=MDBDomainManagerFactory(),
            task_processor_factory=DummyTaskProcessorFactory(),
        )
        sr_future_ref.fulfill(new_sr)
        created_service_registries.add(new_sr)
        return new_sr

    yield factory
    for sr in created_service_registries:
        loop.run_until_complete(sr.close_async())


@pytest.fixture(scope="function")
def sync_service_registry_factory(
    loop,
    rqe_config_subprocess,
    connectors_settings,
    mutation_async_redis,
) -> Callable[[SROptions], ServicesRegistry]:
    created_service_registries = set()

    def sync_sr_factory(opts: SROptions):
        sr_future_ref: FutureRef[DefaultServicesRegistry] = FutureRef()
        reporting_registry = DefaultReportingRegistry(rci=opts.rci)
        mdb_domain_mgr_factory = MDBDomainManagerFactory(
            settings=MDBDomainManagerSettings(
                managed_network_enabled=True,
                mdb_domains=(".mdb.cloud-preprod.yandex.net",),
                mdb_cname_domains=tuple(),
                renaming_map={".mdb.cloud-preprod.yandex.net": ".db.yandex.net"},
            )
        )
        new_sr = DefaultServicesRegistry(
            rci=opts.rci,
            reporting_registry=reporting_registry,
            conn_exec_factory=DefaultConnExecutorFactory(
                async_env=False,
                services_registry_ref=sr_future_ref,
                rqe_config=rqe_config_subprocess,
                conn_sec_mgr=InsecureConnectionSecurityManager(),
                mdb_mgr=mdb_domain_mgr_factory.get_manager(),
                tpe=None,
            ),
            caches_redis_client_factory=None,
            mutations_redis_client_factory=None,
            mutations_cache_factory=None,
            data_processor_service_factory=None,
            connectors_settings=connectors_settings,
            mdb_domain_manager_factory=mdb_domain_mgr_factory,
            task_processor_factory=DummyTaskProcessorFactory(),
        )
        sr_future_ref.fulfill(new_sr)
        created_service_registries.add(new_sr)
        return new_sr

    yield sync_sr_factory
    for sr in created_service_registries:
        sr.close()


@pytest.fixture(scope="function")
def default_service_registry(bi_context, sync_service_registry_factory) -> ServicesRegistry:
    return sync_service_registry_factory(SROptions(rci=bi_context))


@pytest.fixture(scope="function")
def default_async_service_registry(async_service_registry_factory, bi_context) -> ServicesRegistry:
    return async_service_registry_factory(
        SROptions(
            rci=bi_context,
        )
    )


@pytest.fixture(scope="function")
def default_sync_usm(loop, bi_context, default_service_registry, tests_config) -> SyncUSManager:
    us_config = tests_config.CORE_TEST_CONFIG.get_us_config()
    return SyncUSManager(
        us_base_url=us_config.us_host,
        us_auth_context=USAuthContextMaster(us_config.us_master_token),
        crypto_keys_config=tests_config.CORE_TEST_CONFIG.get_crypto_keys_config(),
        bi_context=bi_context,
        services_registry=default_service_registry,
    )


@pytest.fixture(scope="function")
async def local_private_usm(loop, bi_context, qc_async_redis, default_async_service_registry, tests_config):
    us_config = tests_config.CORE_TEST_CONFIG.get_us_config()
    async with AsyncUSManager(
        us_base_url=us_config.us_host,
        us_auth_context=USAuthContextMaster(us_config.us_master_token),
        crypto_keys_config=tests_config.CORE_TEST_CONFIG.get_crypto_keys_config(),
        bi_context=bi_context,
        services_registry=default_async_service_registry,
    ) as usm:
        yield usm


@pytest.fixture(scope="session")
def initdb_ready(tests_config):
    """Synchronization fixture that ensures that initdb has finished. Primarily for `db` tests."""
    return wait_for_initdb(
        initdb_host=tests_config_mod.get_test_container_hostport(
            "init-db",
            fallback_port=50308,
        ).host,
        initdb_port=tests_config_mod.get_test_container_hostport(
            "init-db",
            fallback_port=50308,
        ).port,
    )
