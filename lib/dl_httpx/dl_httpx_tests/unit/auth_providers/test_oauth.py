import dl_httpx


def test_oauth_auth_provider() -> None:
    auth_provider = dl_httpx.OauthAuthProvider(token="test-token")
    assert auth_provider.get_headers() == {"Authorization": "OAuth test-token"}
    assert auth_provider.get_cookies() == {}
