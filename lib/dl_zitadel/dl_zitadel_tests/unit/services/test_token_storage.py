import datetime

import pytest
import pytest_mock

import dl_zitadel
import dl_zitadel.services as dl_zitadel_services


def test_sync_get_token_default(mocker: pytest_mock.MockerFixture) -> None:
    client = mocker.Mock(spec=dl_zitadel_services.SyncClientProtocol)
    service = dl_zitadel_services.ZitadelSyncTokenStorage(
        client=client,
        token_refresh_timeout=datetime.timedelta(seconds=30),
    )

    client.get_token.return_value = dl_zitadel.Token(
        access_token="token",
        expires_in=60,
        request_datetime=datetime.datetime.now(),
    )

    # Multiple calls and only one request
    service.get_token()
    result = service.get_token()

    assert result == "token"

    client.get_token.assert_called_once_with()


@pytest.mark.asyncio
async def test_async_get_token_default(mocker: pytest_mock.MockerFixture) -> None:
    client = mocker.AsyncMock(spec=dl_zitadel_services.AsyncClientProtocol)
    service = dl_zitadel_services.ZitadelAsyncTokenStorage(
        client=client,
        token_refresh_timeout=datetime.timedelta(seconds=30),
    )

    client.get_token.return_value = dl_zitadel.Token(
        access_token="token",
        expires_in=60,
        request_datetime=datetime.datetime.now(),
    )

    # Multiple calls and only one request
    await service.get_token()
    result = await service.get_token()

    assert result == "token"

    client.get_token.assert_awaited_once_with()


def test_sync_get_token_expired(mocker: pytest_mock.MockerFixture) -> None:
    client = mocker.Mock(spec=dl_zitadel_services.SyncClientProtocol)
    service = dl_zitadel_services.ZitadelSyncTokenStorage(
        client=client,
        token_refresh_timeout=datetime.timedelta(seconds=60),
    )

    client.get_token.return_value = dl_zitadel.Token(
        access_token="token",
        expires_in=30,
        request_datetime=datetime.datetime.now(),
    )
    result = service.get_token()

    assert result == "token"
    client.get_token.assert_called_once_with()

    client.get_token.reset_mock()

    client.get_token.return_value = dl_zitadel.Token(
        access_token="token2",
        expires_in=60,
        request_datetime=datetime.datetime.now(),
    )

    result = service.get_token()

    assert result == "token2"
    client.get_token.assert_called_once_with()


@pytest.mark.asyncio
async def test_async_get_token_expired(mocker: pytest_mock.MockerFixture) -> None:
    client = mocker.AsyncMock(spec=dl_zitadel_services.AsyncClientProtocol)
    service = dl_zitadel_services.ZitadelAsyncTokenStorage(
        client=client,
        token_refresh_timeout=datetime.timedelta(seconds=60),
    )

    client.get_token.return_value = dl_zitadel.Token(
        access_token="token",
        expires_in=30,
        request_datetime=datetime.datetime.now(),
    )
    result = await service.get_token()

    assert result == "token"
    client.get_token.assert_awaited_once_with()

    client.get_token.reset_mock()

    client.get_token.return_value = dl_zitadel.Token(
        access_token="token2",
        expires_in=60,
        request_datetime=datetime.datetime.now(),
    )

    result = await service.get_token()

    assert result == "token2"
    client.get_token.assert_awaited_once_with()
