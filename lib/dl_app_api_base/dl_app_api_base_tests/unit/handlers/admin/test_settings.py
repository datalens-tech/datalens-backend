import aiohttp
import pytest


@pytest.mark.asyncio
async def test_default(
    app_client: aiohttp.ClientSession,
) -> None:
    response = await app_client.get("/admin/settings")
    assert response.status == 200
    text = await response.text()
    assert "test_app" in text
