import pytest
import pytest_mock

import dl_auth
from dl_constants.api_constants import (
    DLCookies,
    DLHeadersCommon,
)


class DLTestCookies(DLCookies):
    SESSION = "session"


def test_auth_data_provider_get_headers(mocker: pytest_mock.MockerFixture) -> None:
    mock_auth_data = mocker.MagicMock(spec=dl_auth.AuthData)
    mock_auth_data.get_headers.return_value = {DLHeadersCommon.AUTHORIZATION_TOKEN: "Bearer test-token"}

    provider = dl_auth.AuthDataAuthProvider(
        auth_data=mock_auth_data,
        target=dl_auth.AuthTarget.UNITED_STORAGE,
    )

    assert provider.get_headers() == {"Authorization": "Bearer test-token"}
    mock_auth_data.get_headers.assert_called_once_with(dl_auth.AuthTarget.UNITED_STORAGE)


@pytest.mark.asyncio
async def test_auth_data_provider_get_headers_async(mocker: pytest_mock.MockerFixture) -> None:
    mock_auth_data = mocker.MagicMock(spec=dl_auth.AuthData)
    mock_auth_data.get_headers.return_value = {DLHeadersCommon.AUTHORIZATION_TOKEN: "Bearer test-token"}

    provider = dl_auth.AuthDataAuthProvider(
        auth_data=mock_auth_data,
        target=dl_auth.AuthTarget.UNITED_STORAGE,
    )
    assert await provider.get_headers_async() == {"Authorization": "Bearer test-token"}
    mock_auth_data.get_headers.assert_called_once_with(dl_auth.AuthTarget.UNITED_STORAGE)


def test_auth_data_provider_get_cookies(mocker: pytest_mock.MockerFixture) -> None:
    mock_auth_data = mocker.MagicMock(spec=dl_auth.AuthData)
    mock_auth_data.get_cookies.return_value = {DLTestCookies.SESSION: "test-session-value"}

    provider = dl_auth.AuthDataAuthProvider(
        auth_data=mock_auth_data,
        target=dl_auth.AuthTarget.UNITED_STORAGE,
    )

    assert provider.get_cookies() == {"session": "test-session-value"}
    mock_auth_data.get_cookies.assert_called_once_with(dl_auth.AuthTarget.UNITED_STORAGE)


@pytest.mark.asyncio
async def test_auth_data_provider_get_cookies_async(mocker: pytest_mock.MockerFixture) -> None:
    mock_auth_data = mocker.MagicMock(spec=dl_auth.AuthData)
    mock_auth_data.get_cookies.return_value = {DLTestCookies.SESSION: "test-session-value"}

    provider = dl_auth.AuthDataAuthProvider(
        auth_data=mock_auth_data,
        target=dl_auth.AuthTarget.UNITED_STORAGE,
    )
    assert await provider.get_cookies_async() == {"session": "test-session-value"}
    mock_auth_data.get_cookies.assert_called_once_with(dl_auth.AuthTarget.UNITED_STORAGE)
