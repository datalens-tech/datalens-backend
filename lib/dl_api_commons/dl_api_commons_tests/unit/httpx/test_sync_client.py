import json
from typing import Generator
import unittest.mock

import httpx
import pytest
import pytest_mock
import respx

import dl_api_commons


def test_get_request(
    respx_mock: respx.MockRouter,
) -> None:
    mock_route = respx_mock.get("https://example.com/api/data").respond(
        status_code=200,
        json={"message": "Success", "data": [1, 2, 3]},
        headers={"Content-Type": "application/json"},
    )

    with dl_api_commons.HttpxSyncClient.from_settings(
        dl_api_commons.HttpxClientSettings(base_url="https://example.com"),
    ) as client:
        request = client.prepare_request("GET", "/api/data")
        with client.send(request) as response:
            assert response.status_code == 200
            assert response.json() == {"message": "Success", "data": [1, 2, 3]}
            assert response.headers["Content-Type"] == "application/json"

        assert mock_route.call_count == 1


def test_post_request(
    respx_mock: respx.MockRouter,
) -> None:
    mock_route = respx_mock.post("https://example.com/api/items").respond(
        status_code=201,
        json={"id": 123, "name": "New Item"},
        headers={"Content-Type": "application/json"},
    )

    with dl_api_commons.HttpxSyncClient.from_settings(
        dl_api_commons.HttpxClientSettings(base_url="https://example.com"),
    ) as client:
        payload = {"name": "New Item", "description": "A new item"}
        request = client.prepare_request(
            "POST",
            "/api/items",
            json=payload,
            headers={"Content-Type": "application/json"},
        )
        with client.send(request) as response:
            assert response.status_code == 201
            assert response.json() == {"id": 123, "name": "New Item"}

    assert mock_route.call_count == 1
    request = mock_route.calls.last.request
    assert request.method == "POST"
    assert json.loads(request.content.decode()) == payload
    assert request.headers["Content-Type"] == "application/json"


def test_custom_headers(
    respx_mock: respx.MockRouter,
) -> None:
    mock_route = respx_mock.get("https://example.com/api/secure").respond(status_code=200)
    headers = {"Authorization": "Bearer token123", "X-API-Key": "abc456"}

    with dl_api_commons.HttpxSyncClient.from_settings(
        dl_api_commons.HttpxClientSettings(base_url="https://example.com", base_headers=headers),
    ) as client:
        request = client.prepare_request("GET", "/api/secure")
        with client.send(request) as response:
            assert response.status_code == 200

    assert mock_route.call_count == 1
    request = mock_route.calls.last.request
    assert request.headers["Authorization"] == "Bearer token123"
    assert request.headers["X-API-Key"] == "abc456"


def test_error_handling(
    respx_mock: respx.MockRouter,
) -> None:
    respx_mock.get("https://example.com/api/not-found").respond(status_code=404)
    respx_mock.get("https://example.com/api/forbidden").respond(status_code=403)

    with dl_api_commons.HttpxSyncClient.from_settings(
        dl_api_commons.HttpxClientSettings(base_url="https://example.com"),
    ) as client:
        request = client.prepare_request("GET", "/api/not-found")
        with pytest.raises(dl_api_commons.HttpStatusHttpxClientException) as excinfo:
            with client.send(request):
                pass
        assert excinfo.value.response.status_code == 404

        request = client.prepare_request("GET", "/api/forbidden")
        with pytest.raises(dl_api_commons.HttpStatusHttpxClientException) as excinfo:
            with client.send(request):
                pass
        assert excinfo.value.response.status_code == 403


def test_request_with_params(
    respx_mock: respx.MockRouter,
) -> None:
    mock_route = respx_mock.get("https://example.com/api/search").respond(
        status_code=200,
        json={"results": ["item1", "item2"]},
    )

    with dl_api_commons.HttpxSyncClient.from_settings(
        dl_api_commons.HttpxClientSettings(base_url="https://example.com"),
    ) as client:
        params = {"q": "test", "limit": "10", "offset": "0"}
        request = client.prepare_request("GET", "/api/search", params=params)
        with client.send(request) as response:
            assert response.status_code == 200
            assert response.json() == {"results": ["item1", "item2"]}

    assert mock_route.call_count == 1
    request = mock_route.calls.last.request
    assert dict(request.url.params) == params


def test_cookies_handling(
    respx_mock: respx.MockRouter,
) -> None:
    mock_route = respx_mock.get("https://example.com/api/profile").respond(status_code=200)
    cookies = {"session": "xyz789", "user_id": "123"}

    with dl_api_commons.HttpxSyncClient.from_settings(
        dl_api_commons.HttpxClientSettings(base_url="https://example.com", base_cookies=cookies),
    ) as client:
        request = client.prepare_request("GET", "/api/profile")
        with client.send(request) as response:
            assert response.status_code == 200

    assert mock_route.call_count == 1
    request = mock_route.calls.last.request
    assert request.headers["Cookie"].find("session=xyz789") != -1
    assert request.headers["Cookie"].find("user_id=123") != -1


def test_binary_response(
    respx_mock: respx.MockRouter,
) -> None:
    binary_data = b"binary file content"
    mock_route = respx_mock.get("https://example.com/api/files/download").respond(
        status_code=200,
        content=binary_data,
        headers={"Content-Type": "application/octet-stream"},
    )

    with dl_api_commons.HttpxSyncClient.from_settings(
        dl_api_commons.HttpxClientSettings(base_url="https://example.com"),
    ) as client:
        request = client.prepare_request("GET", "/api/files/download")
        with client.send(request) as response:
            assert response.status_code == 200
            assert response.content == binary_data
            assert response.headers["Content-Type"] == "application/octet-stream"

    assert mock_route.call_count == 1


@pytest.fixture(name="mocked_client")
def fixture_client_with_mocks(
    mock_retry_policy_factory: unittest.mock.Mock,
) -> Generator[dl_api_commons.HttpxSyncClient, None, None]:
    with dl_api_commons.HttpxSyncClient(
        base_url="https://example.com",
        base_cookies={},
        base_headers={},
        retry_policy_factory=mock_retry_policy_factory,
        base_client=httpx.Client(base_url="https://example.com"),
    ) as client:
        yield client


def test_retry_default(
    respx_mock: respx.MockRouter,
    mocked_client: dl_api_commons.HttpxSyncClient,
    mocker: pytest_mock.MockerFixture,
    mock_retry: dl_api_commons.Retry,
    mock_retry_policy: unittest.mock.Mock,
    mock_retry_policy_factory: unittest.mock.Mock,
) -> None:
    status_code = 200
    json_data = {"message": "Success", "data": [1, 2, 3]}
    mock_retry_policy_name = mocker.Mock(spec=str)

    mock_route = respx_mock.get("https://example.com/api/data").respond(
        status_code=status_code,
        json=json_data,
    )

    request = mocked_client.prepare_request("GET", "/api/data")
    with mocked_client.send(request, retry_policy_name=mock_retry_policy_name) as response:
        assert response.status_code == status_code
        assert response.json() == json_data

    assert mock_route.call_count == 1
    assert (
        mock_route.calls.last.request.extensions["timeout"]
        == httpx.Timeout(
            None,
            connect=mock_retry.connect_timeout,
            read=mock_retry.request_timeout,
        ).as_dict()
    )

    mock_retry_policy_factory.get_policy.assert_called_once_with(mock_retry_policy_name)
    mock_retry_policy.iter_retries.assert_called_once()
    mock_retry_policy.can_retry_error.assert_called_once_with(status_code)


def test_retry_retriable_code(
    respx_mock: respx.MockRouter,
    mocked_client: dl_api_commons.HttpxSyncClient,
    mock_retry_policy: unittest.mock.Mock,
) -> None:
    status_code = 200
    json_data = {"message": "Success", "data": [1, 2, 3]}

    mock_route = respx_mock.get("https://example.com/api/data").respond(
        status_code=status_code,
        json=json_data,
    )
    mock_retry_policy.can_retry_error.return_value = True

    request = mocked_client.prepare_request("GET", "/api/data")
    with mocked_client.send(request) as response:
        assert response.status_code == status_code
        assert response.json() == json_data

    assert mock_route.call_count == 3


def test_retry_client_error(
    respx_mock: respx.MockRouter,
    mocked_client: dl_api_commons.HttpxSyncClient,
) -> None:
    base_client_error = httpx.ConnectError("Connection refused")
    mock_route = respx_mock.get("https://example.com/api/data").mock(side_effect=base_client_error)

    request = mocked_client.prepare_request("GET", "/api/data")
    with pytest.raises(dl_api_commons.RequestHttpxClientException) as excinfo:
        with mocked_client.send(request):
            pass

        assert excinfo.value.original_exception, base_client_error

    assert mock_route.call_count == 3


def test_retry_no_retries(
    respx_mock: respx.MockRouter,
    mocked_client: dl_api_commons.HttpxSyncClient,
    mock_retry_policy: unittest.mock.Mock,
) -> None:
    status_code = 200
    json_data = {"message": "Success", "data": [1, 2, 3]}
    mock_route = respx_mock.get("https://example.com/api/data").respond(
        status_code=status_code,
        json=json_data,
    )

    mock_retry_policy.iter_retries.return_value = iter([])

    request = mocked_client.prepare_request("GET", "/api/data")

    with pytest.raises(dl_api_commons.NoRetriesHttpxClientException):
        with mocked_client.send(request):
            pass

    assert mock_route.call_count == 0
