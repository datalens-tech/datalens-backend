import aiohttp
import pytest

import dl_app_api_ext_testing.base as base


class AdminExtTestSuite(base.ExtTestSuiteBase):
    @pytest.mark.asyncio
    async def test_settings(
        self,
        app_client: aiohttp.ClientSession,
        ext_test_suite_settings: base.ExtTestSuiteSettings,
    ) -> None:
        response = await app_client.get("admin/settings")
        assert response.status == 200
        assert ext_test_suite_settings.APP_NAME in await response.text()

    @pytest.mark.asyncio
    async def test_settings_unauthorized(
        self,
        unauthorized_app_client: aiohttp.ClientSession,
    ) -> None:
        response = await unauthorized_app_client.get("admin/settings")
        assert response.status == 401

    @pytest.mark.asyncio
    async def test_dynconfig(
        self,
        app_client: aiohttp.ClientSession,
        ext_test_suite_settings: base.ExtTestSuiteSettings,
    ) -> None:
        response = await app_client.get("admin/dynconfig")
        assert response.status == 200
        assert await response.json() == ext_test_suite_settings.EXPECTED_DYNCONFIG

    @pytest.mark.asyncio
    async def test_dynconfig_unauthorized(
        self,
        unauthorized_app_client: aiohttp.ClientSession,
    ) -> None:
        response = await unauthorized_app_client.get("admin/dynconfig")
        assert response.status == 401
