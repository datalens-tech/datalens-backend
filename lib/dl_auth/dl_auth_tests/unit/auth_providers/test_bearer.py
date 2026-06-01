import pytest

import dl_auth


def test_bearer_auth_provider() -> None:
    auth_provider = dl_auth.BearerAuthProvider(token="test-token")
    assert auth_provider.get_headers() == {"Authorization": "Bearer test-token"}
    assert auth_provider.get_cookies() == {}


@pytest.mark.asyncio
async def test_bearer_auth_provider_async() -> None:
    auth_provider = dl_auth.BearerAuthProvider(token="test-token")
    assert await auth_provider.get_headers_async() == {"Authorization": "Bearer test-token"}
    assert await auth_provider.get_cookies_async() == {}
