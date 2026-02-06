from typing import Annotated

import pydantic

from dl_api_lib.app_settings import (
    AppSettings,
    ControlApiAppSettings,
    DataApiAppSettings,
)
from dl_core.connectors.settings.base import ConnectorSettings
import dl_settings


def testing_postload_connectors_settings(value: dict[str, ConnectorSettings]) -> dict[str, ConnectorSettings]:
    return value


class TestingConnectorsSettingsMixin(AppSettings):
    # Override CONNECTORS field with blank AfterValidator for testing purposes
    CONNECTORS: Annotated[
        dl_settings.TypedDictWithTypeKeyAnnotation[ConnectorSettings],
        pydantic.AfterValidator(testing_postload_connectors_settings),
    ] = pydantic.Field(default_factory=dict)


class TestingControlApiAppSettings(TestingConnectorsSettingsMixin, ControlApiAppSettings):
    ...


class TestingDataApiAppSettings(TestingConnectorsSettingsMixin, DataApiAppSettings):
    ...
