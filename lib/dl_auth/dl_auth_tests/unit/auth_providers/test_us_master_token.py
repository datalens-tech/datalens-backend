import pytest

import dl_auth


def test_us_master_token_auth_provider() -> None:
    us_master_token = "test-us-master-token"
    auth_provider = dl_auth.USMasterTokenAuthProvider(token=us_master_token)
    assert auth_provider.get_headers() == {"X-US-Master-Token": us_master_token}
    assert auth_provider.get_cookies() == {}


@pytest.mark.asyncio
async def test_us_master_token_auth_provider_async() -> None:
    us_master_token = "test-us-master-token"
    auth_provider = dl_auth.USMasterTokenAuthProvider(token=us_master_token)
    assert await auth_provider.get_headers_async() == {"X-US-Master-Token": "test-us-master-token"}
    assert await auth_provider.get_cookies_async() == {}
