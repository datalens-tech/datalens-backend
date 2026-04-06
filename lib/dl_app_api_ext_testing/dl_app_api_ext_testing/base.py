import abc
from collections.abc import AsyncGenerator
from typing import Any

import aiohttp
import pytest
import pytest_asyncio

import dl_settings


class ReadinessSubsystemSettings(dl_settings.BaseSettings):
    NAME: str
    CRITICAL: bool


class ExtTestSuiteSettings(dl_settings.BaseSettings):
    BASE_URL: str
    APP_NAME: str
    APP_VERSION: str
    EXPECTED_DYNCONFIG: dict[str, Any]
    READINESS_SUBSYSTEMS: list[ReadinessSubsystemSettings]


class ExtTestSuiteBase(abc.ABC):
    @pytest_asyncio.fixture(name="default_app_client")
    async def fixture_default_app_client(
        self,
        ext_test_suite_settings: ExtTestSuiteSettings,
    ) -> AsyncGenerator[aiohttp.ClientSession, None]:
        async with aiohttp.ClientSession(
            base_url=ext_test_suite_settings.BASE_URL,
        ) as session:
            yield session

    @pytest_asyncio.fixture(name="app_client")
    async def fixture_app_client(
        self,
        default_app_client: aiohttp.ClientSession,
    ) -> AsyncGenerator[aiohttp.ClientSession, None]:
        yield default_app_client

    @pytest_asyncio.fixture(name="unauthorized_app_client")
    async def fixture_unauthorized_app_client(
        self,
        ext_test_suite_settings: ExtTestSuiteSettings,
    ) -> AsyncGenerator[aiohttp.ClientSession, None]:
        pytest.skip("unauthorized_app_client is not set, skipping unauthorized test")
