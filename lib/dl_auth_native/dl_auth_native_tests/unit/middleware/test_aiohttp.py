import datetime
import typing
import unittest.mock as mock

import aiohttp.pytest_plugin as aiohttp_pytest_plugin
import aiohttp.web as aiohttp_web
import pytest
import pytest_asyncio

import dl_api_commons.aio.middlewares as dl_api_commons_aio_middlewares
import dl_auth_native


AiohttpHandler = typing.Callable[[aiohttp_web.Request], typing.Awaitable[aiohttp_web.Response]]


@pytest.fixture(name="handler")
def fixture_handler() -> AiohttpHandler:
    async def handler(request: aiohttp_web.Request) -> aiohttp_web.Response:
        assert request["bi_request_context_info_temp"].user_id == "test_user_id"
        return aiohttp_web.Response(body=b"OK")

    return handler


@pytest.fixture(name="aiohttp_app")
def fixture_aiohttp_app(
    token_decoder: dl_auth_native.DecoderProtocol,
    handler: AiohttpHandler,
) -> aiohttp_web.Application:
    middleware = dl_auth_native.AioHTTPMiddleware(token_decoder)

    app = aiohttp_web.Application()
    app.middlewares.extend(
        [
            dl_api_commons_aio_middlewares.RequestBootstrap(
                req_id_service=dl_api_commons_aio_middlewares.RequestId(),
            ).middleware,
            middleware.get_middleware(),
        ]
    )
    app.router.add_get("/", handler)

    return app


@pytest_asyncio.fixture(name="aiohttp_test_client")
async def fixture_aiohttp_test_client(
    aiohttp_app: aiohttp_web.Application,
    aiohttp_client: aiohttp_pytest_plugin.AiohttpClient,
) -> aiohttp_pytest_plugin.TestClient:
    return await aiohttp_client(aiohttp_app)


@pytest.mark.asyncio
async def test_default(
    aiohttp_test_client: aiohttp_pytest_plugin.TestClient,
    token_decoder: mock.Mock,
) -> None:
    token_decoder.decode.return_value = dl_auth_native.Payload(
        user_id="test_user_id",
        expires_at=datetime.datetime.now(),
    )
    response = await aiohttp_test_client.get(
        "/",
        headers={
            "Authorization": "Bearer token",
        },
    )

    assert token_decoder.decode.called_once_with("token")
    assert response.status == 200


@pytest.mark.asyncio
async def test_missing_token_header(
    aiohttp_test_client: aiohttp_pytest_plugin.TestClient,
) -> None:
    response = await aiohttp_test_client.get("/")

    assert response.status == 401
    assert await response.text() == "401: User access token header is missing"


@pytest.mark.asyncio
async def test_bad_token_type(
    aiohttp_test_client: aiohttp_pytest_plugin.TestClient,
) -> None:
    response = await aiohttp_test_client.get(
        "/",
        headers={
            "Authorization": "Basic token",
        },
    )

    assert response.status == 401
    assert await response.text() == "401: Bad token type"


@pytest.mark.asyncio
async def test_invalid_token(
    aiohttp_test_client: aiohttp_pytest_plugin.TestClient,
    token_decoder: mock.Mock,
) -> None:
    token_decoder.decode.side_effect = dl_auth_native.DecodeError("Invalid token")

    response = await aiohttp_test_client.get(
        "/",
        headers={
            "Authorization": "Bearer token",
        },
    )

    assert token_decoder.decode.called_once_with("token")
    assert response.status == 401
    assert await response.text() == "401: Invalid user access token: Invalid token"
