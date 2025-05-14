import asyncio
from typing import TYPE_CHECKING
import uuid

import pytest
import redis.asyncio

from dl_api_commons.base_models import RequestContextInfo
from dl_configs.crypto_keys import (
    CryptoKeysConfig,
    get_dummy_crypto_keys_config,
)
from dl_configs.enums import RedisMode
from dl_configs.settings_submodels import RedisSettings
from dl_file_uploader_lib.redis_model.base import RedisModelManager
from dl_s3.s3_service import S3Service
from dl_testing.constants import TEST_USER_ID
from dl_testing.containers import get_test_container_hostport


if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client as SyncS3Client


@pytest.fixture(scope="function")
def rci() -> RequestContextInfo:
    return RequestContextInfo(user_id=TEST_USER_ID)


@pytest.fixture(scope="session")
def crypto_keys_config() -> CryptoKeysConfig:
    return get_dummy_crypto_keys_config()


@pytest.fixture(scope="session")
def redis_app_settings() -> RedisSettings:
    return RedisSettings(  # type: ignore  # 2025-04-28 # TODO: Unexpected keyword argument "MODE" for "RedisSettings"  [call-arg]
        MODE=RedisMode.single_host,
        CLUSTER_NAME="",
        HOSTS=(get_test_container_hostport("redis", fallback_port=52404).host,),
        PORT=get_test_container_hostport("redis", fallback_port=52404).port,
        DB=9,
    )


@pytest.fixture(scope="function")
def redis_cli(redis_app_settings: RedisSettings) -> redis.asyncio.Redis:
    return redis.asyncio.Redis(
        host=redis_app_settings.HOSTS[0],
        port=redis_app_settings.PORT,
        db=redis_app_settings.DB,
        password=redis_app_settings.PASSWORD,
    )


@pytest.fixture(autouse=True)
def loop(event_loop: asyncio.AbstractEventLoop) -> asyncio.AbstractEventLoop:
    asyncio.set_event_loop(event_loop)
    return event_loop


@pytest.fixture(scope="session")
def tenant_id() -> str:
    return uuid.uuid4().hex


@pytest.fixture(scope="function")
async def s3_service() -> S3Service:
    s3_host = get_test_container_hostport("s3-storage", fallback_port=52222).host
    s3_port = get_test_container_hostport("s3-storage", fallback_port=52222).port

    service = S3Service(
        access_key_id="accessKey1",
        secret_access_key="verySecretKey1",
        endpoint_url=f"http://{s3_host}:{s3_port}",
        use_virtual_host_addressing=False,
        tmp_bucket_name="bucket-tmp",
        persistent_bucket_name="bucket-persistent",
    )

    await service.initialize()

    client: SyncS3Client = service.get_sync_client()

    try:
        client.create_bucket(Bucket="bucket-tmp")
    except client.exceptions.ClientError:
        pass

    try:
        client.create_bucket(Bucket="bucket-persistent")
    except client.exceptions.ClientError:
        pass

    return service


@pytest.fixture(scope="function")
def redis_model_manager(
    redis_cli: redis.asyncio.Redis,
    rci: RequestContextInfo,
    crypto_keys_config: CryptoKeysConfig,
) -> RedisModelManager:
    return RedisModelManager(redis=redis_cli, rci=rci, crypto_keys_config=crypto_keys_config)
