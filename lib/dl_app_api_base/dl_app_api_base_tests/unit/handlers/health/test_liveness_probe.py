import aiohttp
import pytest

import dl_app_api_base.app


@pytest.mark.asyncio
async def test_default(
    app_settings: dl_app_api_base.app.HttpServerAppSettingsMixin,
) -> None:
    async with aiohttp.ClientSession() as session:
        response = await session.get(
            f"http://{app_settings.HTTP_SERVER.HOST}:{app_settings.HTTP_SERVER.PORT}/api/v1/health/liveness"
        )
        assert response.status == 200
        assert await response.json() == {"status": "healthy"}
