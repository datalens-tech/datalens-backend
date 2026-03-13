import aiohttp
import pytest


@pytest.mark.asyncio
async def test_default(
    app_client: aiohttp.ClientSession,
) -> None:
    response = await app_client.get("/admin/dynconfig")
    assert response.status == 200
    assert await response.json() == {"source_type": "null", "config": {}}
