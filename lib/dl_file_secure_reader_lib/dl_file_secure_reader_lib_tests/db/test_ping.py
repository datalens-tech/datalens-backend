from aiohttp.pytest_plugin import TestClient
import pytest


@pytest.mark.asyncio
async def test_ping(web_app: TestClient) -> None:
    resp = await web_app.get("/reader/ping")
    assert resp.status == 200
