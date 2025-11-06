import dl_auth


def test_oauth_auth_provider() -> None:
    auth_provider = dl_auth.OauthAuthProvider(token="test-token")
    assert auth_provider.get_headers() == {"Authorization": "OAuth test-token"}
    assert auth_provider.get_cookies() == {}
