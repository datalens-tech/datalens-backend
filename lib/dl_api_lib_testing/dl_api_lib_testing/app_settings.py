from importlib.resources import files
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


def _default_test_us_client_settings() -> USClientSettings:
    # MASTER_TOKEN_AUTHORIZATION_ENABLED=True: test apps emit BOTH the JWT and the legacy
    # X-US-Master-Token header. OSS US containers (configured with BI_MASTER_TOKEN_PUBLIC_KEY_PRIMARY)
    # verify via JWT; private/EAS US containers accept the static token.
    dynamic_auth_private_key = (
        files("dl_core_testing") / "keys" / "dynamic_us_master_token_private_key.pem"
    ).read_text()
    return USClientSettings(
        DYNAMIC_AUTH_PRIVATE_KEY=dynamic_auth_private_key,
        MASTER_TOKEN_AUTHORIZATION_ENABLED=True,
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
