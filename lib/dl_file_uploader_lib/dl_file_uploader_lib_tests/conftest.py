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
from dl_testing.constants import TEST_USER_ID
from dl_testing.containers import get_test_container_hostport


pytest_plugins = ("aiohttp.pytest_plugin",)


@pytest.fixture(scope="function")
def rci() -> RequestContextInfo:
    return RequestContextInfo(user_id=TEST_USER_ID)


@pytest.fixture(scope="session")
def crypto_keys_config() -> CryptoKeysConfig:
    return get_dummy_crypto_keys_config()


@pytest.fixture(scope="session")
def redis_app_settings() -> RedisSettings:
    return RedisSettings(
        MODE=RedisMode.single_host,
        CLUSTER_NAME="",
        HOSTS=(get_test_container_hostport("redis", fallback_port=52404).host,),
        PORT=get_test_container_hostport("redis", fallback_port=52404).port,
        DB=9,
    )


@pytest.fixture(scope="function")
def redis_cli(redis_app_settings) -> redis.asyncio.Redis:
    return redis.asyncio.Redis(
        host=redis_app_settings.HOSTS[0],
        port=redis_app_settings.PORT,
        db=redis_app_settings.DB,
        password=redis_app_settings.PASSWORD,
    )


@pytest.fixture(scope="function")
def redis_model_manager(redis_cli, rci, crypto_keys_config) -> RedisModelManager:
    return RedisModelManager(redis=redis_cli, rci=rci, crypto_keys_config=crypto_keys_config)
