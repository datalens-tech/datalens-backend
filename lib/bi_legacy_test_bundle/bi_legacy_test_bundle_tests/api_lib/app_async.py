from __future__ import annotations

from typing import Type

from aiohttp import web

from bi_api_lib.app.data_api.app import DataApiAppFactory
from bi_configs.connectors_settings import ConnectorSettingsBase
from bi_constants.enums import ConnectionType

from bi_api_lib.app_settings import DataApiAppSettings
from bi_api_lib.loader import ApiLibraryConfig, load_bi_api_lib
from bi_api_lib_testing_ya.app import TestingDataApiAppFactoryPrivate


def create_app(
        setting: DataApiAppSettings,
        connectors_settings: dict[ConnectionType, ConnectorSettingsBase],
        app_factory_cls: Type[DataApiAppFactory] = TestingDataApiAppFactoryPrivate,
) -> web.Application:
    data_api_app_factory = app_factory_cls(settings=setting)
    load_bi_api_lib(ApiLibraryConfig(api_connector_ep_names=setting.BI_API_CONNECTOR_WHITELIST))
    return data_api_app_factory.create_app(
        connectors_settings=connectors_settings,
    )
