import os

import aiohttp.web
import pytest

from dl_configs.crypto_keys import get_dummy_crypto_keys_config
from dl_configs.settings_submodels import (
    GoogleAppSettings,
    S3Settings,
)
from dl_file_secure_reader_lib.app import create_app as create_reader_app
from dl_file_uploader_worker_lib.settings import (
    DeprecatedFileUploaderWorkerSettings,
    FileUploaderWorkerSettings,
)
from dl_testing.env_params.generic import GenericEnvParamGetter


@pytest.fixture(scope="session")
def env_param_getter() -> GenericEnvParamGetter:
    filepath = os.path.join(os.path.dirname(__file__), "params.yml")
    return GenericEnvParamGetter.from_yaml_file(filepath)


# overriding top-level fixture to set google app settings that are only needed in ext tests
@pytest.fixture(scope="session")
def file_uploader_worker_settings(
    env_param_getter,
    redis_app_settings,
    redis_arq_settings,
    s3_settings,
    connectors_settings,
    us_config,
    secure_reader,
):
    deprecated_settings = DeprecatedFileUploaderWorkerSettings(
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
            API_KEY=env_param_getter.get_str_value("GOOGLE_API_KEY"),
            CLIENT_ID="dummy",  # TODO test auth properly
            CLIENT_SECRET="dummy",
        ),
        CRYPTO_KEYS_CONFIG=get_dummy_crypto_keys_config(),
        SECURE_READER=secure_reader,
    )
    settings = FileUploaderWorkerSettings(fallback=deprecated_settings)
    yield settings


@pytest.fixture(scope="function")
def reader_app(loop, secure_reader):
    current_app = create_reader_app()
    runner = aiohttp.web.AppRunner(current_app)
    loop.run_until_complete(runner.setup())
    site = aiohttp.web.UnixSite(runner, path=secure_reader.SOCKET)
    return loop.run_until_complete(site.start())
