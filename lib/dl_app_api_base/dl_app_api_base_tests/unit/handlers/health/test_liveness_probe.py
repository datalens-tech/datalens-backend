import aiohttp
import pytest


@pytest.mark.asyncio
async def test_default(
    app_client: aiohttp.ClientSession,
) -> None:
    response = await app_client.get("/api/v1/health/liveness")
    assert response.status == 200
    assert await response.json() == {"status": "healthy"}
