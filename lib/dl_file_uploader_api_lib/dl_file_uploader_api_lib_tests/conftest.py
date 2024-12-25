from __future__ import annotations

import asyncio
import logging
import os
import sys
from typing import (
    TYPE_CHECKING,
    Any,
)

import aiohttp.pytest_plugin
import aiohttp.test_utils
from aiohttp.typedefs import Middleware
import aiohttp.web
import attr
import pytest
import redis.asyncio

from dl_api_commons.aio.middlewares.auth_trust_middleware import auth_trust_middleware
from dl_api_commons.base_models import (
    NoAuthData,
    RequestContextInfo,
    TenantCommon,
)
from dl_api_commons.client.common import DLCommonAPIClient
from dl_configs.crypto_keys import (
    CryptoKeysConfig,
    get_dummy_crypto_keys_config,
)
from dl_configs.enums import RedisMode
from dl_configs.settings_submodels import (
    CorsSettings,
    CsrfSettings,
    GoogleAppSettings,
    RedisSettings,
    S3Settings,
)
from dl_constants.api_constants import DLHeadersCommon
from dl_core.loader import (
    CoreLibraryConfig,
    load_core_lib,
)
from dl_core.services_registry.top_level import DummyServiceRegistry
from dl_core.united_storage_client import USAuthContextMaster
from dl_core.us_manager.us_manager_async import AsyncUSManager
from dl_core_testing.environment import (
    common_pytest_configure,
    prepare_united_storage,
)
from dl_file_secure_reader_lib.app import create_app as create_reader_app
from dl_file_uploader_api_lib.app import FileUploaderApiAppFactory
from dl_file_uploader_api_lib.dl_request import FileUploaderDLRequest
from dl_file_uploader_api_lib.settings import FileUploaderAPISettings
from dl_file_uploader_api_lib_tests.config import (
    CONNECTOR_WHITELIST,
    TestingUSConfig,
)
from dl_file_uploader_lib.redis_model.base import RedisModelManager
from dl_file_uploader_task_interface.context import FileUploaderTaskContext
from dl_file_uploader_worker_lib.settings import (
    FileUploaderConnectorsSettings,
    FileUploaderWorkerSettings,
    SecureReader,
)
from dl_file_uploader_worker_lib.testing.task_processor_client import get_task_processor_client
from dl_task_processor.processor import TaskProcessor
from dl_task_processor.state import (
    BITaskStateImpl,
    TaskState,
)
from dl_testing.containers import get_test_container_hostport
from dl_testing.s3_utils import (
    create_s3_bucket,
    create_s3_client,
)
from dl_testing.utils import (
    get_default_aiohttp_session,
    get_root_certificates,
    wait_for_initdb,
)

from dl_connector_bundle_chs3.chs3_base.core.settings import FileS3ConnectorSettings


if TYPE_CHECKING:
    from types_aiobotocore_s3 import S3Client as AsyncS3Client


LOGGER = logging.getLogger(__name__)


pytest_plugins = ("aiohttp.pytest_plugin",)

try:
    del aiohttp.pytest_plugin.loop
except AttributeError:
    pass


_TESTS_USER_ID = "_the_tests_asyncapp_user_id_"
_TESTS_USER_NAME = "_the_tests_asyncapp_user_name_"


def pytest_configure(config: Any) -> None:  # noqa
    common_pytest_configure(tracing_service_name="tests_bi_file_uploader")


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
    return wait_for_initdb(initdb_port=51408)


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


@pytest.fixture(scope="session", autouse=True)
def loaded_libraries():
    load_core_lib(core_lib_config=CoreLibraryConfig(core_connector_ep_names=CONNECTOR_WHITELIST))


@pytest.fixture(scope="session")
def redis_app_settings() -> RedisSettings:
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
def crypto_keys_config() -> CryptoKeysConfig:
    return get_dummy_crypto_keys_config()


@pytest.fixture(scope="function")
def app_settings(monkeypatch, redis_app_settings, redis_arq_settings, s3_settings, crypto_keys_config):
    monkeypatch.setenv("EXT_QUERY_EXECUTER_SECRET_KEY", "dummy")

    settings = FileUploaderAPISettings(
        REDIS_APP=redis_app_settings,
        REDIS_ARQ=redis_arq_settings,
        CORS=CorsSettings(
            ALLOWED_ORIGINS=("https://foo.bar", "https://fee.bar"),
            ALLOWED_HEADERS=("Content-Type", "X-Request-ID", "x-csrf-token", "x-unauthorized"),
            EXPOSE_HEADERS=("X-Request-ID",),
        ),
        CSRF=CsrfSettings(
            METHODS=("POST", "PUT", "DELETE"),
            HEADER_NAME="x-csrf-token",
            TIME_LIMIT=3600 * 12,
            SECRET="123321",
        ),
        S3=S3Settings(
            ENDPOINT_URL=s3_settings.ENDPOINT_URL,
            ACCESS_KEY_ID=s3_settings.ACCESS_KEY_ID,
            SECRET_ACCESS_KEY=s3_settings.SECRET_ACCESS_KEY,
        ),
        S3_TMP_BUCKET_NAME="bi-file-uploader-tmp",
        S3_PERSISTENT_BUCKET_NAME="bi-file-uploader",
        FILE_UPLOADER_MASTER_TOKEN="valid-master-token",
        CRYPTO_KEYS_CONFIG=crypto_keys_config,
        ALLOW_XLSX=True,
    )
    yield settings


class TestingFileUploaderApiAppFactory(FileUploaderApiAppFactory[FileUploaderAPISettings]):
    def get_auth_middlewares(self) -> list[Middleware]:
        return [
            auth_trust_middleware(
                fake_user_id=_TESTS_USER_ID,
                fake_user_name=_TESTS_USER_NAME,
            )
        ]


@pytest.fixture(scope="function")
def bi_file_uploader_app(loop, aiohttp_client, app_settings):
    app_factory = TestingFileUploaderApiAppFactory(settings=app_settings)
    app = app_factory.create_app("tests")
    return loop.run_until_complete(aiohttp_client(app))


@pytest.fixture(scope="function")
async def fu_client(bi_file_uploader_app) -> DLCommonAPIClient:
    async with get_default_aiohttp_session() as session:
        yield DLCommonAPIClient(
            base_url=f"http://{bi_file_uploader_app.host}:{bi_file_uploader_app.port}",
            tenant=TenantCommon(),
            auth_data=NoAuthData(),
            session=session,
        )


@pytest.fixture(scope="function")
async def s3_client(s3_settings) -> AsyncS3Client:
    async with create_s3_client(s3_settings) as client:
        yield client


@pytest.fixture(scope="function")
async def s3_tmp_bucket(initdb_ready, s3_client, app_settings) -> str:
    bucket_name = app_settings.S3_TMP_BUCKET_NAME
    await create_s3_bucket(s3_client, bucket_name, max_attempts=1)
    return bucket_name


@pytest.fixture(scope="function")
async def s3_persistent_bucket(initdb_ready, s3_client, app_settings) -> str:
    bucket_name = app_settings.S3_PERSISTENT_BUCKET_NAME
    await create_s3_bucket(s3_client, bucket_name, max_attempts=1)
    return bucket_name


@pytest.fixture(scope="function")
def redis_cli(redis_app_settings) -> redis.asyncio.Redis:
    return redis.asyncio.Redis(
        host=redis_app_settings.HOSTS[0],
        port=redis_app_settings.PORT,
        db=redis_app_settings.DB,
        password=redis_app_settings.PASSWORD,
    )


@pytest.fixture(scope="function")
def rci() -> RequestContextInfo:
    return RequestContextInfo(user_id="_the_tests_asyncapp_user_id_")


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


@pytest.fixture(scope="function")
def redis_model_manager(redis_cli, rci, crypto_keys_config) -> RedisModelManager:
    return RedisModelManager(redis=redis_cli, rci=rci, crypto_keys_config=crypto_keys_config)


@pytest.fixture(scope="function")
def master_token_header(app_settings) -> dict[DLHeadersCommon, str]:
    return {DLHeadersCommon.FILE_UPLOADER_MASTER_TOKEN: app_settings.FILE_UPLOADER_MASTER_TOKEN}


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


@pytest.fixture(scope="function")
def file_uploader_worker_settings(
    redis_app_settings,
    redis_arq_settings,
    s3_settings,
    connectors_settings,
    us_config,
    crypto_keys_config,
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
        CRYPTO_KEYS_CONFIG=crypto_keys_config,
        SECURE_READER=secure_reader,
    )
    yield settings


@pytest.fixture(scope="session")
def root_certificates() -> bytes:
    return get_root_certificates()


@pytest.fixture(scope="function", params=["local"], ids=["local_tp"])
async def use_local_task_processor(request, monkeypatch, loop, file_uploader_worker_settings, root_certificates):
    task_state = TaskState(BITaskStateImpl())
    async with get_task_processor_client(
        client_type=request.param,
        loop=loop,
        task_state=task_state,
        file_uploader_worker_settings=file_uploader_worker_settings,
        ca_data=root_certificates,
    ) as task_processor_client:

        def new_get_task_processor(self: FileUploaderDLRequest) -> TaskProcessor:
            task_processor_client._request_id = self.rci.request_id
            return task_processor_client

        def new_make_task_processor(self, request_id: str) -> TaskProcessor:
            task_processor_client._request_id = request_id
            return task_processor_client

        monkeypatch.setattr(FileUploaderDLRequest, "get_task_processor", new_get_task_processor)
        monkeypatch.setattr(FileUploaderTaskContext, "make_task_processor", new_make_task_processor)

        yield


@pytest.fixture(scope="session")
def prepare_us(us_config):
    prepare_united_storage(
        us_host=us_config.base_url,
        us_master_token=us_config.master_token,
        us_pg_dsn=us_config.psycopg2_pg_dns,
        force=us_config.force_clear_db_on_launch,
    )

    return us_config


@pytest.fixture(scope="function")
@pytest.mark.usefixtures("loop")
def default_async_usm_per_test(bi_context, prepare_us, us_config, root_certificates):
    rci = RequestContextInfo.create_empty()
    return AsyncUSManager(
        us_base_url=us_config.base_url,
        us_auth_context=USAuthContextMaster(us_config.master_token),
        crypto_keys_config=us_config.crypto_keys_config,
        bi_context=bi_context,
        services_registry=DummyServiceRegistry(rci=rci),
        ca_data=root_certificates,
    )


@pytest.fixture(scope="function")
def reader_app(loop, secure_reader):
    current_app = create_reader_app()
    runner = aiohttp.web.AppRunner(current_app)
    loop.run_until_complete(runner.setup())
    site = aiohttp.web.UnixSite(runner, path=secure_reader.SOCKET)
    return loop.run_until_complete(site.start())


@pytest.fixture(scope="session")
def ya_docs_oauth_token(env_param_getter):
    return env_param_getter.get_str_value("YA_DOCS_API_KEY")
