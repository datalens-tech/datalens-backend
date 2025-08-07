import httpx
import pytest

import dl_zitadel


def test_sync_get_token_invalid_client_secret(bi_zitadel_sync_client: dl_zitadel.ZitadelSyncClient) -> None:
    bi_zitadel_sync_client._client_secret = "invalid"
    with pytest.raises(httpx.HTTPStatusError):
        bi_zitadel_sync_client.get_token()


@pytest.mark.asyncio
async def test_async_get_token_invalid_client_secret(bi_zitadel_async_client: dl_zitadel.ZitadelAsyncClient) -> None:
    bi_zitadel_async_client._client_secret = "invalid"
    with pytest.raises(httpx.HTTPStatusError):
        await bi_zitadel_async_client.get_token()


def test_sync_get_token(bi_zitadel_sync_client: dl_zitadel.ZitadelSyncClient) -> None:
    result = bi_zitadel_sync_client.get_token()
    assert isinstance(result.access_token, str)


@pytest.mark.asyncio
async def test_async_get_token(bi_zitadel_async_client: dl_zitadel.ZitadelAsyncClient) -> None:
    result = await bi_zitadel_async_client.get_token()
    assert isinstance(result.access_token, str)


def test_sync_introspect(bi_zitadel_sync_client: dl_zitadel.ZitadelSyncClient, charts_access_token: str) -> None:
    result = bi_zitadel_sync_client.introspect(token=charts_access_token)
    assert result.active is True
    assert result.username == "charts"


@pytest.mark.asyncio
async def test_async_introspect(
    bi_zitadel_async_client: dl_zitadel.ZitadelAsyncClient,
    charts_access_token: str,
) -> None:
    result = await bi_zitadel_async_client.introspect(token=charts_access_token)
    assert result.active is True
    assert result.username == "charts"


def test_sync_introspect_invalid_token(bi_zitadel_sync_client: dl_zitadel.ZitadelSyncClient) -> None:
    result = bi_zitadel_sync_client.introspect(token="invalid")
    assert result.active is False


@pytest.mark.asyncio
async def test_async_introspect_invalid_token(bi_zitadel_async_client: dl_zitadel.ZitadelAsyncClient) -> None:
    result = await bi_zitadel_async_client.introspect(token="invalid")
    assert result.active is False
