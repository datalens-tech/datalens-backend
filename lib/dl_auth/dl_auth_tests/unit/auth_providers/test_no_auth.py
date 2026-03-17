import pytest

import dl_auth


def test_no_auth_provider() -> None:
    auth_provider = dl_auth.NoAuthProvider()
    assert auth_provider.get_headers() == {}
    assert auth_provider.get_cookies() == {}


@pytest.mark.asyncio
async def test_no_auth_provider_async() -> None:
    auth_provider = dl_auth.NoAuthProvider()
    assert await auth_provider.get_headers_async() == {}
    assert await auth_provider.get_cookies_async() == {}
