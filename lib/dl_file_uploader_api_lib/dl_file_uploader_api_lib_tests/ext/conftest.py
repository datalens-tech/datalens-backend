import pytest
import pytest_asyncio

from dl_configs.settings_submodels import GoogleAppSettings
from dl_file_uploader_api_lib_tests.ext.settings import Settings
from dl_file_uploader_worker_lib.settings import (
    DeprecatedFileUploaderWorkerSettings,
    FileUploaderWorkerSettings,
)
from dl_s3.s3_service import S3ClientSettings


@pytest_asyncio.fixture(scope="function", autouse=True)
async def use_local_task_processor_auto(use_local_task_processor):
    yield


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings()


# overriding top-level fixture to set google app settings that are only needed in ext tests
@pytest.fixture(scope="function")
def file_uploader_worker_settings(
    settings,
    redis_app_settings,
    redis_arq_settings,
    s3_settings,
    connectors_settings,
    us_config,
    crypto_keys_config,
    secure_reader,
):
    deprecated_settings = DeprecatedFileUploaderWorkerSettings(
        REDIS_APP=redis_app_settings,
        REDIS_ARQ=redis_arq_settings,
        S3_TMP_BUCKET_NAME="bi-file-uploader-tmp",
        S3_PERSISTENT_BUCKET_NAME="bi-file-uploader",
        SENTRY_DSN=None,
        US_BASE_URL=us_config.base_url,
        US_MASTER_TOKEN=us_config.master_token,
        GSHEETS_APP=GoogleAppSettings(
            API_KEY=settings.GOOGLE_API_KEY,
            CLIENT_ID="dummy",  # TODO test auth properly
            CLIENT_SECRET="dummy",
        ),
        SECURE_READER=secure_reader,
        CRYPTO_KEYS_CONFIG=crypto_keys_config,
    )
    worker_settings = FileUploaderWorkerSettings(
        fallback=deprecated_settings,
        CONNECTORS=connectors_settings,
        S3=S3ClientSettings(
            ENDPOINT_URL=s3_settings.ENDPOINT_URL,
            ACCESS_KEY_ID=s3_settings.ACCESS_KEY_ID,
            SECRET_ACCESS_KEY=s3_settings.SECRET_ACCESS_KEY,
        ),
    )
    yield worker_settings
