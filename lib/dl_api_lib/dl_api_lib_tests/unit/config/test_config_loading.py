from pathlib import Path

import attr
import pytest

from dl_api_lib.app_settings import (
    CachesTTLSettings,
    ControlApiAppSettings,
    DataApiAppSettings,
)
from dl_api_lib.connector_availability.base import ConnectorAvailabilityConfig
from dl_api_lib_tests.unit import config as test_directory
from dl_configs.connector_availability import (
    ConnectorAvailabilityConfigSettings,
    ConnectorIconSrc,
    ConnectorSettings,
)
from dl_configs.crypto_keys import CryptoKeysConfig
from dl_configs.rqe import RQEConfig
from dl_configs.settings_loaders.loader_env import (
    load_connectors_settings_from_env_with_fallback,
    load_settings_from_env_with_fallback,
)
from dl_core.core_connectors import load_all_connectors

from dl_connector_bundle_chs3.chs3_base.core.settings import FileS3ConnectorSettings
from dl_connector_bundle_chs3.file.core.constants import CONNECTION_TYPE_FILE
from dl_connector_bundle_chs3.file.core.settings import file_s3_settings_fallback


TEST_CONFIG_PATH = Path(test_directory.__file__).parent / "config.yaml"

CRYPTO_KEYS_CONFIG = CryptoKeysConfig(
    actual_key_id="1",
    map_id_key={"1": "asd", "0": "asdasd"},
)
RQE_CONFIG = RQEConfig.get_default().clone(hmac_key=b"123")
US_HOST = "http://us:8083"
US_MASTER_TOKEN = "fake-us-master-token"

EXPECTED_CONTROL_API_SETTINGS = ControlApiAppSettings(
    CONNECTOR_AVAILABILITY=ConnectorAvailabilityConfig(),
    CRYPTO_KEYS_CONFIG=CRYPTO_KEYS_CONFIG,
    RQE_CONFIG=RQE_CONFIG,
    RQE_FORCE_OFF=True,
    US_BASE_URL=US_HOST,
    US_MASTER_TOKEN=US_MASTER_TOKEN,
)
EXPECTED_DATA_API_SETTINGS = DataApiAppSettings(
    CACHES_TTL_SETTINGS=CachesTTLSettings(MATERIALIZED=3600, OTHER=300),
    CRYPTO_KEYS_CONFIG=CRYPTO_KEYS_CONFIG,
    RQE_CONFIG=RQE_CONFIG,
    RQE_FORCE_OFF=True,
    US_BASE_URL=US_HOST,
    US_MASTER_TOKEN=US_MASTER_TOKEN,
)

EXPECTED_FILE_SETTINGS = FileS3ConnectorSettings(
    SECURE=True,
    HOST="localhost",
    PORT=8443,
    USERNAME="datalens",
    PASSWORD="qwerty",
    ACCESS_KEY_ID="key_id",
    SECRET_ACCESS_KEY="access_key",
    BUCKET="bucket",
    S3_ENDPOINT="http://s3-storage:8000",
)


@pytest.fixture
def expected_settings(request):
    if isinstance(request.param, ControlApiAppSettings):
        # preload connectors first as ConnectorAvailabilityConfig contains connection types
        load_all_connectors()
        connector_availability = ConnectorAvailabilityConfig.from_settings(
            ConnectorAvailabilityConfigSettings(
                uncategorized=[
                    ConnectorSettings(conn_type="clickhouse"),
                    ConnectorSettings(conn_type="postgres"),
                ],
                visible_connectors={"clickhouse"},
                icon_src=ConnectorIconSrc(data="blank"),
            )
        )
        return attr.evolve(request.param, CONNECTOR_AVAILABILITY=connector_availability)
    return request.param


@pytest.mark.parametrize(
    "expected_settings",
    argvalues=(EXPECTED_CONTROL_API_SETTINGS, EXPECTED_DATA_API_SETTINGS),
    ids=("control_api", "data_api"),
    indirect=True,
)
def test_config_loading(expected_settings):
    env = dict(
        CONFIG_PATH=TEST_CONFIG_PATH,
        EXT_QUERY_EXECUTER_SECRET_KEY="123",
        DL_CRY_ACTUAL_KEY_ID="1",
        DL_CRY_FALLBACK_KEY_ID="0",
        DL_CRY_KEY_VAL_ID_0="asdasd",
        DL_CRY_KEY_VAL_ID_1="asd",
        US_HOST=US_HOST,
        US_MASTER_TOKEN=US_MASTER_TOKEN,
    )
    settings = load_settings_from_env_with_fallback(settings_type=expected_settings.__class__, env=env)
    assert settings == expected_settings


def test_connector_settings_loading():
    load_all_connectors()
    env = dict(
        CONFIG_PATH=TEST_CONFIG_PATH,
        CONNECTORS_FILE_PASSWORD="qwerty",
        CONNECTORS_FILE_ACCESS_KEY_ID="key_id",
        CONNECTORS_FILE_SECRET_ACCESS_KEY="access_key",
    )
    connectors_settings = load_connectors_settings_from_env_with_fallback(
        settings_registry={CONNECTION_TYPE_FILE: FileS3ConnectorSettings},
        fallbacks={CONNECTION_TYPE_FILE: file_s3_settings_fallback},
        env=env,
    )

    assert len(connectors_settings) == 1
    file_settings = attr.evolve(
        connectors_settings[CONNECTION_TYPE_FILE],
        REPLACE_SECRET_SALT=EXPECTED_FILE_SETTINGS.REPLACE_SECRET_SALT,  # non-deterministic
    )
    assert file_settings == EXPECTED_FILE_SETTINGS
