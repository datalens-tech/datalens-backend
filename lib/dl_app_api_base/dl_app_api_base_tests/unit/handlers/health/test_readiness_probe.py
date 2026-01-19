import aiohttp
import pytest

import dl_app_api_base.app
import dl_app_api_base_tests.unit.conftest as conftest


@pytest.mark.asyncio
async def test_default(
    app_settings: dl_app_api_base.app.HttpServerAppSettingsMixin,
    readiness_resource: conftest.ReadinessResource,
) -> None:
    readiness_resource.set_readiness(True)

    async with aiohttp.ClientSession() as session:
        response = await session.get(
            f"http://{app_settings.HTTP_SERVER.HOST}:{app_settings.HTTP_SERVER.PORT}/api/v1/health/readiness"
        )
        assert response.status == 200
        assert await response.json() == {
            "status": "healthy",
            "subsystems_status": {
                "readiness_resource.is_ready": True,
            },
        }


@pytest.mark.asyncio
async def test_unhealthy(
    app_settings: dl_app_api_base.app.HttpServerAppSettingsMixin,
    readiness_resource: conftest.ReadinessResource,
) -> None:
    readiness_resource.set_readiness(False)

    async with aiohttp.ClientSession() as session:
        response = await session.get(
            f"http://{app_settings.HTTP_SERVER.HOST}:{app_settings.HTTP_SERVER.PORT}/api/v1/health/readiness"
        )
        assert response.status == 500
        assert await response.json() == {
            "status": "unhealthy",
            "subsystems_status": {
                "readiness_resource.is_ready": False,
            },
        }
