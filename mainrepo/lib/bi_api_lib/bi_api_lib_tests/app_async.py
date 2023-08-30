from __future__ import annotations

from typing import Optional

from aiohttp import web

from bi_configs.connectors_settings import ConnectorSettingsBase
from bi_constants.enums import ConnectionType

from bi_api_lib.app_settings import AsyncAppSettings, TestAppSettings
from bi_api_lib.loader import load_bi_api_lib
from bi_api_lib_testing.app import TestingDataApiAppFactory


def create_app(
        setting: AsyncAppSettings,
        connectors_settings: dict[ConnectionType, ConnectorSettingsBase],
        test_setting: Optional[TestAppSettings] = None
) -> web.Application:
    data_api_app_factory = TestingDataApiAppFactory()
    load_bi_api_lib()
    return data_api_app_factory.create_app(
        setting=setting,
        connectors_settings=connectors_settings,
        test_setting=test_setting,
    )
