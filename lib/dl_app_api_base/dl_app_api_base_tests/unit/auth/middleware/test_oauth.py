import http

import aiohttp
import pytest


@pytest.mark.asyncio
async def test_default(
    app_client: aiohttp.ClientSession,
    oauth_user1_token: str,
) -> None:
    response = await app_client.get(
        "/api/v1/oauth/ping",
        headers={
            "Authorization": f"Bearer {oauth_user1_token}",
        },
    )
    assert response.status == http.HTTPStatus.OK


@pytest.mark.asyncio
async def test_invalid_token(
    app_client: aiohttp.ClientSession,
) -> None:
    response = await app_client.get(
        "/api/v1/oauth/ping",
        headers={
            "Authorization": "Bearer invalid_token",
        },
    )
    assert response.status == http.HTTPStatus.UNAUTHORIZED


@pytest.mark.asyncio
async def test_missing_token(
    app_client: aiohttp.ClientSession,
) -> None:
    response = await app_client.get(
        "/api/v1/oauth/ping",
    )
    assert response.status == http.HTTPStatus.UNAUTHORIZED


@pytest.mark.asyncio
async def test_applicable_but_not_found(
    app_client: aiohttp.ClientSession,
    oauth_user1_token: str,
) -> None:
    response = await app_client.get(
        "/api/v1/oauth/not_found",
        headers={
            "Authorization": f"Bearer {oauth_user1_token}",
        },
    )
    assert response.status == http.HTTPStatus.NOT_FOUND
