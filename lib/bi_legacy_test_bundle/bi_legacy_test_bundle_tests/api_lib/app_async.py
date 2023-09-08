from __future__ import annotations

from typing import Optional

from aiohttp import web

from bi_configs.connectors_settings import ConnectorSettingsBase
from bi_constants.enums import ConnectionType

from bi_api_lib.app_settings import TestAppSettings, DataApiAppSettings
from bi_api_lib.loader import ApiLibraryConfig, load_bi_api_lib
from bi_api_lib_testing_ya.app import TestingDataApiAppFactoryPrivate


def create_app(
        setting: DataApiAppSettings,
        connectors_settings: dict[ConnectionType, ConnectorSettingsBase],
        test_setting: Optional[TestAppSettings] = None
) -> web.Application:
    data_api_app_factory = TestingDataApiAppFactoryPrivate(settings=setting)
    load_bi_api_lib(ApiLibraryConfig(api_connector_ep_names=setting.BI_API_CONNECTOR_WHITELIST))
    return data_api_app_factory.create_app(
        connectors_settings=connectors_settings,
        test_setting=test_setting,
    )
