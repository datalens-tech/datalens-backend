from __future__ import annotations

from typing import Optional

from aiohttp import web


from bi_api_lib.app_settings import AsyncAppSettings, TestAppSettings
from bi_api_lib_testing.app import TestingDataApiAppFactory


def create_app(setting: AsyncAppSettings, test_setting: Optional[TestAppSettings] = None) -> web.Application:
    data_api_app_factory = TestingDataApiAppFactory()
    return data_api_app_factory.create_app(
        setting=setting,
        test_setting=test_setting,
    )
