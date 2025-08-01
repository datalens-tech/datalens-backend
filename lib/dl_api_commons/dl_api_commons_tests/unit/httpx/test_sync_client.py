import json

import httpx
import pytest
import respx

import dl_api_commons


class TestBIHttpxSyncClient:
    @respx.mock
    def test_get_request(self) -> None:
        mock_route = respx.get("https://example.com/api/data").respond(
            status_code=200,
            json={"message": "Success", "data": [1, 2, 3]},
            headers={"Content-Type": "application/json"},
        )

        with dl_api_commons.BIHttpxSyncClient.from_settings(
            dl_api_commons.BIHttpxClientSettings(base_url="https://example.com"),
        ) as client:
            request = client._base_client.build_request("GET", "/api/data")
            with client.send(request) as response:
                assert response.status_code == 200
                assert response.json() == {"message": "Success", "data": [1, 2, 3]}
                assert response.headers["Content-Type"] == "application/json"

                assert mock_route.called
                assert mock_route.call_count == 1

    @respx.mock
    def test_post_request(self) -> None:
        mock_route = respx.post("https://example.com/api/items").respond(
            status_code=201,
            json={"id": 123, "name": "New Item"},
            headers={"Content-Type": "application/json"},
        )

        with dl_api_commons.BIHttpxSyncClient.from_settings(
            dl_api_commons.BIHttpxClientSettings(base_url="https://example.com"),
        ) as client:
            payload = {"name": "New Item", "description": "A new item"}
            request = client._base_client.build_request(
                "POST",
                "/api/items",
                json=payload,
                headers={"Content-Type": "application/json"},
            )
            with client.send(request) as response:
                assert response.status_code == 201
                assert response.json() == {"id": 123, "name": "New Item"}

                assert mock_route.called
                assert mock_route.call_count == 1
                request = mock_route.calls.last.request
                assert request.method == "POST"
                assert json.loads(request.content.decode()) == payload
                assert request.headers["Content-Type"] == "application/json"

    @respx.mock
    def test_custom_headers(self) -> None:
        mock_route = respx.get("https://example.com/api/secure").respond(status_code=200)
        headers = {"Authorization": "Bearer token123", "X-API-Key": "abc456"}

        with dl_api_commons.BIHttpxSyncClient.from_settings(
            dl_api_commons.BIHttpxClientSettings(base_url="https://example.com", base_headers=headers),
        ) as client:
            request = client._base_client.build_request("GET", "/api/secure")
            with client.send(request) as response:
                assert response.status_code == 200

                assert mock_route.called
                request = mock_route.calls.last.request
                assert request.headers["Authorization"] == "Bearer token123"
                assert request.headers["X-API-Key"] == "abc456"

    @respx.mock
    def test_error_handling(self) -> None:
        respx.get("https://example.com/api/not-found").respond(status_code=404)
        respx.get("https://example.com/api/server-error").respond(status_code=500)
        respx.get("https://example.com/api/forbidden").respond(status_code=403)

        with dl_api_commons.BIHttpxSyncClient.from_settings(
            dl_api_commons.BIHttpxClientSettings(base_url="https://example.com"),
        ) as client:
            request = client._base_client.build_request("GET", "/api/not-found")
            with pytest.raises(httpx.HTTPStatusError) as excinfo:
                with client.send(request):
                    pass
            assert excinfo.value.response.status_code == 404

            request = client._base_client.build_request("GET", "/api/server-error")
            with pytest.raises(httpx.HTTPStatusError) as excinfo:
                with client.send(request):
                    pass
            assert excinfo.value.response.status_code == 500

            request = client._base_client.build_request("GET", "/api/forbidden")
            with pytest.raises(httpx.HTTPStatusError) as excinfo:
                with client.send(request):
                    pass
            assert excinfo.value.response.status_code == 403

    @respx.mock
    def test_request_with_params(self) -> None:
        mock_route = respx.get("https://example.com/api/search").respond(
            status_code=200,
            json={"results": ["item1", "item2"]},
        )

        with dl_api_commons.BIHttpxSyncClient.from_settings(
            dl_api_commons.BIHttpxClientSettings(base_url="https://example.com"),
        ) as client:
            params = {"q": "test", "limit": "10", "offset": "0"}
            request = client._base_client.build_request("GET", "/api/search", params=params)
            with client.send(request) as response:
                assert response.status_code == 200
                assert response.json() == {"results": ["item1", "item2"]}

                assert mock_route.called
                request = mock_route.calls[0].request
                assert dict(request.url.params) == params

    @respx.mock
    def test_cookies_handling(self) -> None:
        mock_route = respx.get("https://example.com/api/profile").respond(status_code=200)
        cookies = {"session": "xyz789", "user_id": "123"}

        with dl_api_commons.BIHttpxSyncClient.from_settings(
            dl_api_commons.BIHttpxClientSettings(base_url="https://example.com", base_cookies=cookies),
        ) as client:
            request = client._base_client.build_request("GET", "/api/profile")
            with client.send(request) as response:
                assert response.status_code == 200

                assert mock_route.called
                request = mock_route.calls.last.request
                assert request.headers["Cookie"].find("session=xyz789") != -1
                assert request.headers["Cookie"].find("user_id=123") != -1

    @respx.mock
    def test_binary_response(self) -> None:
        binary_data = b"binary file content"
        mock_route = respx.get("https://example.com/api/files/download").respond(
            status_code=200,
            content=binary_data,
            headers={"Content-Type": "application/octet-stream"},
        )

        with dl_api_commons.BIHttpxSyncClient.from_settings(
            dl_api_commons.BIHttpxClientSettings(base_url="https://example.com"),
        ) as client:
            request = client._base_client.build_request("GET", "/api/files/download")
            with client.send(request) as response:
                assert response.status_code == 200
                assert response.content == binary_data
                assert response.headers["Content-Type"] == "application/octet-stream"

                assert mock_route.called

    @respx.mock
    def test_timeout_handling(self) -> None:
        mock_route = respx.get("https://example.com/api/slow").mock(
            side_effect=httpx.TimeoutException("Connection timed out"),
        )

        with dl_api_commons.BIHttpxSyncClient.from_settings(
            dl_api_commons.BIHttpxClientSettings(base_url="https://example.com"),
        ) as client:
            request = client._base_client.build_request("GET", "/api/slow")
            with pytest.raises(httpx.TimeoutException):
                with client.send(request):
                    pass

            assert mock_route.called

    @respx.mock
    def test_connection_error_handling(self) -> None:
        mock_route = respx.get("https://example.com/api/unreachable").mock(
            side_effect=httpx.ConnectError("Connection refused"),
        )

        with dl_api_commons.BIHttpxSyncClient.from_settings(
            dl_api_commons.BIHttpxClientSettings(base_url="https://example.com"),
        ) as client:
            request = client._base_client.build_request("GET", "/api/unreachable")
            with pytest.raises(httpx.ConnectError):
                with client.send(request):
                    pass

            assert mock_route.called
