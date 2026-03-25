import dl_auth
from dl_constants.api_constants import DLHeadersCommon
from dl_file_uploader_api_lib.auth_provider import AuthDataUSAuthProvider


class _FakeAuthData(dl_auth.AuthData):
    def get_headers(self, target: dl_auth.AuthTarget | None = None) -> dict:
        if target == dl_auth.AuthTarget.UNITED_STORAGE:
            return {DLHeadersCommon.AUTHORIZATION_TOKEN: "Bearer test-token"}
        return {}

    def get_cookies(self, target: dl_auth.AuthTarget | None = None) -> dict:
        return {}


def test_auth_data_us_auth_provider_headers():
    auth_data = _FakeAuthData()
    provider = AuthDataUSAuthProvider(auth_data=auth_data)
    headers = provider.get_headers()
    assert headers == {"Authorization": "Bearer test-token"}


def test_auth_data_us_auth_provider_empty_cookies():
    auth_data = _FakeAuthData()
    provider = AuthDataUSAuthProvider(auth_data=auth_data)
    cookies = provider.get_cookies()
    assert cookies == {}


def test_auth_data_us_auth_provider_no_auth():
    provider = AuthDataUSAuthProvider(auth_data=dl_auth.NoAuthData())
    assert provider.get_headers() == {}
    assert provider.get_cookies() == {}
