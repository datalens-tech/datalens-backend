import aiohttp
import pytest


@pytest.mark.asyncio
async def test_default(
    app_client: aiohttp.ClientSession,
) -> None:
    response = await app_client.get("/admin/dyn_config")
    assert response.status == 200
    assert await response.json() == {"source_type": "null", "config": {}}
