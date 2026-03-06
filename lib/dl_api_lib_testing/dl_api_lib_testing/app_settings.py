from pathlib import Path
from typing import Annotated

import pydantic

from dl_api_lib.app_settings import (
    AppSettings,
    ControlApiAppSettings,
    DataApiAppSettings,
)
from dl_core.connectors.settings.base import ConnectorSettings
from dl_core.us_manager.settings import USClientSettings
import dl_settings


TESTENV_COMMON_DIR = Path(__file__).resolve().parents[2] / "testenv-common"
TEST_DYNAMIC_AUTH_PRIVATE_KEY = (TESTENV_COMMON_DIR / "keys" / "dynamic_us_master_token_private_key.pem").read_text()


def _default_test_us_client_settings() -> USClientSettings:
    return USClientSettings(
        DYNAMIC_AUTH_PRIVATE_KEY=TEST_DYNAMIC_AUTH_PRIVATE_KEY,
    )


def testing_postload_connectors_settings(value: dict[str, ConnectorSettings]) -> dict[str, ConnectorSettings]:
    return value


class TestingConnectorsSettingsMixin(AppSettings):
    # Override CONNECTORS field with blank AfterValidator for testing purposes
    CONNECTORS: Annotated[
        dl_settings.TypedDictWithTypeKeyAnnotation[ConnectorSettings],
        pydantic.AfterValidator(testing_postload_connectors_settings),
    ] = pydantic.Field(default_factory=dict)


class TestingControlApiAppSettings(TestingConnectorsSettingsMixin, ControlApiAppSettings):
    US_CLIENT: USClientSettings = pydantic.Field(default_factory=_default_test_us_client_settings)


class TestingDataApiAppSettings(TestingConnectorsSettingsMixin, DataApiAppSettings):
    US_CLIENT: USClientSettings = pydantic.Field(default_factory=_default_test_us_client_settings)
