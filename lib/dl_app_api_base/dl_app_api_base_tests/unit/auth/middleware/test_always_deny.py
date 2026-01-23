import http

import aiohttp
import pytest


@pytest.mark.asyncio
async def test_default(
    app_client: aiohttp.ClientSession,
) -> None:
    response = await app_client.get(
        "/api/v1/always_deny/ping",
    )
    assert response.status == http.HTTPStatus.UNAUTHORIZED
