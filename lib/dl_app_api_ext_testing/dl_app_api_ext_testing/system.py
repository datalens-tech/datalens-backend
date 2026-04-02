import aiohttp
import pytest

import dl_app_api_ext_testing.base as base


class SystemExtTestSuite(base.ExtTestSuiteBase):
    @pytest.mark.asyncio
    async def test_app_info(
        self,
        app_client: aiohttp.ClientSession,
        ext_test_suite_settings: base.ExtTestSuiteSettings,
    ) -> None:
        response = await app_client.get("system/app-info")
        assert response.status == 200
        assert await response.json() == {
            "app_name": ext_test_suite_settings.APP_NAME,
            "version": ext_test_suite_settings.APP_VERSION,
        }

    @pytest.mark.asyncio
    async def test_liveness_probe(self, app_client: aiohttp.ClientSession) -> None:
        response = await app_client.get("system/health/liveness")
        assert response.status == 200
        assert await response.json() == {
            "status": "healthy",
        }

    @pytest.mark.asyncio
    async def test_readiness_probe(
        self,
        app_client: aiohttp.ClientSession,
        ext_test_suite_settings: base.ExtTestSuiteSettings,
    ) -> None:
        response = await app_client.get("system/health/readiness")
        assert response.status == 200
        assert await response.json() == {
            "status": "healthy",
            "subsystems_status": {
                subsystem.NAME: {"value": True, "critical": subsystem.CRITICAL}
                for subsystem in ext_test_suite_settings.READINESS_SUBSYSTEMS
            },
        }

    @pytest.mark.asyncio
    async def test_startup_probe(
        self,
        app_client: aiohttp.ClientSession,
        ext_test_suite_settings: base.ExtTestSuiteSettings,
    ) -> None:
        response = await app_client.get("system/health/startup")
        assert response.status == 200
        assert await response.json() == {
            "status": "healthy",
            "subsystems_status": {
                subsystem.NAME: {"value": True, "critical": subsystem.CRITICAL}
                for subsystem in ext_test_suite_settings.READINESS_SUBSYSTEMS
            },
        }
