import dl_httpx


def test_no_auth_provider() -> None:
    auth_provider = dl_httpx.NoAuthProvider()
    assert auth_provider.get_headers() == {}
    assert auth_provider.get_cookies() == {}
