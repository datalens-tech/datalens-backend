import http

import aiohttp
import pytest


@pytest.mark.asyncio
async def test_default(
    app_client: aiohttp.ClientSession,
) -> None:
    response = await app_client.get(
        "/api/v1/no_auth/ping",
    )
    assert response.status == http.HTTPStatus.OK
