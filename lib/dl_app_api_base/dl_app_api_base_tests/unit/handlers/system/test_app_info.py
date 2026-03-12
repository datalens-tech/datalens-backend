import aiohttp
import pytest


@pytest.mark.asyncio
async def test_default(
    app_client: aiohttp.ClientSession,
) -> None:
    response = await app_client.get("/system/app-info")
    assert response.status == 200
    assert await response.json() == {"app_name": "test_app", "version": "test_app:1.0.0"}
