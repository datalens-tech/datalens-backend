from __future__ import annotations

import asyncio
import logging
import os
import sys
from typing import TYPE_CHECKING

import attr
import pytest
import redis.asyncio

from dl_api_commons.base_models import (
    RequestContextInfo,
    TenantCommon,
)
from dl_configs.crypto_keys import get_dummy_crypto_keys_config
from dl_configs.enums import RedisMode
from dl_configs.settings_submodels import (
    GoogleAppSettings,
    RedisSettings,
    S3Settings,
)
from dl_core.loader import (
    CoreLibraryConfig,
    load_core_lib,
)
from dl_core.services_registry.top_level import DummyServiceRegistry
from dl_core.united_storage_client import USAuthContextMaster
from dl_core.us_manager.us_manager_async import AsyncUSManager
from dl_core.us_manager.us_manager_sync import SyncUSManager
from dl_core_testing.environment import (
    common_pytest_configure,
    prepare_united_storage,
)
from dl_file_uploader_lib.redis_model.base import RedisModelManager
from dl_file_uploader_worker_lib.app import FileUploaderContextFab
from dl_file_uploader_worker_lib.settings import (
    FileUploaderConnectorsSettings,
    FileUploaderWorkerSettings,
    SecureReader,
)
from dl_file_uploader_worker_lib.tasks import REGISTRY
from dl_file_uploader_worker_lib.testing.app_factory import TestingFileUploaderWorkerFactory
from dl_file_uploader_worker_lib_tests.config import (
    CONNECTOR_WHITELIST,
    TestingUSConfig,
)
from dl_task_processor.arq_wrapper import (
    create_arq_redis_settings,
    create_redis_pool,
)
from dl_task_processor.executor import Executor
from dl_task_processor.processor import (
    ARQProcessorImpl,
    LocalProcessorImpl,
    TaskProcessor,
)
from dl_task_processor.state import (
    BITaskStateImpl,
    TaskState,
)
from dl_task_processor.worker import ArqWorkerTestWrapper
from dl_testing.containers import get_test_container_hostport
from dl_testing.s3_utils import (
    create_s3_bucket,
    create_s3_client,
    create_sync_s3_client,
)
from dl_testing.utils import wait_for_initdb

from dl_connector_bundle_chs3.chs3_base.core.settings import FileS3ConnectorSettings


if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client as SyncS3Client
    from types_aiobotocore_s3 import S3Client as AsyncS3Client


LOGGER = logging.getLogger(__name__)


pytest_plugins = ("aiohttp.pytest_plugin",)


def pytest_configure(config: Any) -> None:  # noqa
    common_pytest_configure(tracing_service_name="tests_bi_file_uploader_worker")

    os.environ["EXT_QUERY_EXECUTER_SECRET_KEY"] = "qwerty"


@pytest.fixture(autouse=True)
def loop(event_loop):
    """
    Preventing creation of new loop by `aiohttp.pytest_plugin` loop fixture in favor of pytest-asyncio one
    And set loop pytest-asyncio created loop as default for thread
    """
    asyncio.set_event_loop(event_loop)
    return event_loop


@pytest.fixture(scope="session")
def initdb_ready():
    return wait_for_initdb(initdb_port=get_test_container_hostport("init-db", fallback_port=51508).port)


@pytest.fixture(scope="function")
def rci() -> RequestContextInfo:
    return RequestContextInfo(user_id="_the_tests_asyncapp_user_id_")


@pytest.fixture(scope="session", autouse=True)
def loaded_libraries():
    load_core_lib(core_lib_config=CoreLibraryConfig(core_connector_ep_names=CONNECTOR_WHITELIST))


@pytest.fixture(scope="session")
def us_config():
    return TestingUSConfig(
        base_url=f"http://{get_test_container_hostport('us').as_pair()}",
        psycopg2_pg_dns=(
            f'host={get_test_container_hostport("pg-us").host}'
            f' port={get_test_container_hostport("pg-us").port}'
            " user=us"
            " password=us"
            " dbname=us-db-ci_purgeable"
        ),
    )


@pytest.fixture(scope="session")
def redis_app_settings():
    return RedisSettings(
        MODE=RedisMode.single_host,
        CLUSTER_NAME="",
        HOSTS=(get_test_container_hostport("redis").host,),
        PORT=get_test_container_hostport("redis").port,
        DB=9,
    )


@pytest.fixture(scope="session")
def redis_arq_settings(redis_app_settings):
    return attr.evolve(redis_app_settings, DB=11)


@pytest.fixture(scope="session")
def s3_settings() -> S3Settings:
    return S3Settings(
        ENDPOINT_URL=f'http://{get_test_container_hostport("s3-storage").as_pair()}',
        # default access and secrets for zenko/cloudserver
        ACCESS_KEY_ID="accessKey1",
        SECRET_ACCESS_KEY="verySecretKey1",
    )


@pytest.fixture(scope="session")
def secure_reader():
    socket_name = "reader.sock"
    if sys.platform == "darwin":
        path = "/var/tmp"
    else:
        path = "/var"

    return SecureReader(
        SOCKET=os.path.join(path, socket_name),
    )


@pytest.fixture(scope="session")
def connectors_settings(s3_settings):
    return FileUploaderConnectorsSettings(
        FILE=FileS3ConnectorSettings(
            SECURE=False,
            HOST=get_test_container_hostport("db-clickhouse", original_port=8123).host,
            PORT=get_test_container_hostport("db-clickhouse", original_port=8123).port,
            USERNAME="datalens",
            PASSWORD="qwerty",
            ACCESS_KEY_ID=s3_settings.ACCESS_KEY_ID,
            SECRET_ACCESS_KEY=s3_settings.SECRET_ACCESS_KEY,
            BUCKET="bi-file-uploader",
            S3_ENDPOINT="http://s3-storage:8000",
        ),
    )


@pytest.fixture(scope="session")
def file_uploader_worker_settings(
    redis_app_settings,
    redis_arq_settings,
    s3_settings,
    connectors_settings,
    us_config,
    secure_reader,
):
    settings = FileUploaderWorkerSettings(
        REDIS_APP=redis_app_settings,
        REDIS_ARQ=redis_arq_settings,
        S3=S3Settings(
            ENDPOINT_URL=s3_settings.ENDPOINT_URL,
            ACCESS_KEY_ID=s3_settings.ACCESS_KEY_ID,
            SECRET_ACCESS_KEY=s3_settings.SECRET_ACCESS_KEY,
        ),
        S3_TMP_BUCKET_NAME="bi-file-uploader-tmp",
        S3_PERSISTENT_BUCKET_NAME="bi-file-uploader",
        SENTRY_DSN=None,
        US_BASE_URL=us_config.base_url,
        US_MASTER_TOKEN=us_config.master_token,
        CONNECTORS=connectors_settings,
        GSHEETS_APP=GoogleAppSettings(
            API_KEY="dummy",
            CLIENT_ID="dummy",
            CLIENT_SECRET="dummy",
        ),
        CRYPTO_KEYS_CONFIG=get_dummy_crypto_keys_config(),
        SECURE_READER=secure_reader,
    )
    yield settings


@pytest.fixture(scope="function")
async def redis_pool(file_uploader_worker_settings):
    pool = await create_redis_pool(
        create_arq_redis_settings(file_uploader_worker_settings.REDIS_ARQ),
    )
    yield pool
    await pool.close()


@pytest.fixture(scope="function")
def task_state():
    return TaskState(BITaskStateImpl())


@pytest.fixture(scope="function")
async def task_processor_arq_worker(loop, task_state, file_uploader_worker_settings):
    LOGGER.info("Set up worker")
    worker = TestingFileUploaderWorkerFactory(settings=file_uploader_worker_settings).create_worker(state=task_state)
    wrapper = ArqWorkerTestWrapper(loop=loop, worker=worker)
    yield await wrapper.start()
    await wrapper.stop()


@pytest.fixture(scope="function")
def task_processor_arq_client(loop, task_processor_arq_worker, redis_pool, task_state):
    impl = ARQProcessorImpl(redis_pool)
    p = TaskProcessor(impl=impl, state=task_state)
    LOGGER.info("Arq TP is ready")
    return p


@pytest.fixture(scope="function")
async def task_processor_local_client(loop, task_state, file_uploader_worker_settings):
    context_fab = FileUploaderContextFab(file_uploader_worker_settings)
    context = await context_fab.make()
    executor = Executor(context=context, state=task_state, registry=REGISTRY)
    impl = LocalProcessorImpl(executor)
    processor = TaskProcessor(impl=impl, state=task_state)
    LOGGER.info("Local TP is ready")
    yield processor
    await context_fab.tear_down(context)


@pytest.fixture(scope="function", params=["local"])
def task_processor_client(request, task_processor_arq_client, task_processor_local_client):
    if request.param == "arq":
        return task_processor_arq_client
    elif request.param == "local":
        return task_processor_local_client


@pytest.fixture(scope="function")
async def s3_client(s3_settings) -> AsyncS3Client:
    async with create_s3_client(s3_settings) as client:
        yield client


@pytest.fixture(scope="function")
def s3_client_sync(s3_settings) -> SyncS3Client:
    return create_sync_s3_client(s3_settings)


@pytest.fixture(scope="function")
def redis_cli(redis_app_settings) -> redis.asyncio.Redis:
    return redis.asyncio.Redis(
        host=redis_app_settings.HOSTS[0],
        port=redis_app_settings.PORT,
        db=redis_app_settings.DB,
        password=redis_app_settings.PASSWORD,
    )


@pytest.fixture(scope="function")
def redis_model_manager(redis_cli, rci) -> RedisModelManager:
    return RedisModelManager(redis=redis_cli, rci=rci, crypto_keys_config=get_dummy_crypto_keys_config())


@pytest.fixture(scope="function")
async def s3_tmp_bucket(initdb_ready, s3_client, file_uploader_worker_settings) -> str:
    bucket_name = file_uploader_worker_settings.S3_TMP_BUCKET_NAME
    await create_s3_bucket(s3_client, bucket_name, max_attempts=1)
    return bucket_name


@pytest.fixture(scope="function")
async def s3_persistent_bucket(initdb_ready, s3_client, file_uploader_worker_settings) -> str:
    bucket_name = file_uploader_worker_settings.S3_PERSISTENT_BUCKET_NAME
    await create_s3_bucket(s3_client, bucket_name, max_attempts=1)
    return bucket_name


@pytest.fixture(scope="session")
def bi_context() -> RequestContextInfo:
    return RequestContextInfo.create(
        request_id=None,
        tenant=TenantCommon(),
        user_name=None,
        user_id="_the_tests_fixture_user_id_",
        x_dl_debug_mode=None,
        secret_headers=None,
        auth_data=None,
        plain_headers=None,
        endpoint_code=None,
        x_dl_context=None,
    )


@pytest.fixture(scope="session")
def prepare_us(us_config):
    prepare_united_storage(
        us_host=us_config.base_url,
        us_master_token=us_config.master_token,
        us_pg_dsn=us_config.psycopg2_pg_dns,
        force=us_config.force_clear_db_on_launch,
    )

    return us_config


@pytest.fixture(scope="session")
def default_sync_usm(bi_context, prepare_us, us_config):
    rci = RequestContextInfo.create_empty()
    return SyncUSManager(
        us_base_url=us_config.base_url,
        us_auth_context=USAuthContextMaster(us_config.master_token),
        bi_context=bi_context,
        crypto_keys_config=get_dummy_crypto_keys_config(),
        services_registry=DummyServiceRegistry(rci=rci),
    )


@pytest.fixture(scope="function")
@pytest.mark.usefixtures("loop")
def default_async_usm_per_test(bi_context, prepare_us, us_config):
    rci = RequestContextInfo.create_empty()
    return AsyncUSManager(
        us_base_url=us_config.base_url,
        us_auth_context=USAuthContextMaster(us_config.master_token),
        crypto_keys_config=us_config.crypto_keys_config,
        bi_context=bi_context,
        services_registry=DummyServiceRegistry(rci=rci),
    )
