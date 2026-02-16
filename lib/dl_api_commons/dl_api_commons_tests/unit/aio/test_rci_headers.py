from aiohttp import web
from aiohttp.test_utils import TestClient
from multidict import CIMultiDict
import pytest

import dl_api_commons.aio.middlewares as dl_api_commons_aio_middlewares
from dl_api_commons.aiohttp.aiohttp_wrappers import DLRequestView


@pytest.mark.asyncio
async def test_rci_headers_populates_without_commit(aiohttp_client: TestClient) -> None:
    app = web.Application(
        middlewares=[
            dl_api_commons_aio_middlewares.RequestBootstrap(
                req_id_service=dl_api_commons_aio_middlewares.RequestId(),
            ).middleware,
            dl_api_commons_aio_middlewares.rci_headers_middleware(
                rci_extra_plain_headers=("X-Custom-Plain",),
                rci_extra_secret_headers=("X-Custom-Secret",),
            ),
        ]
    )

    class TestView(DLRequestView):
        async def get(self) -> web.Response:
            dl_req = self.dl_request
            assert not dl_req.is_rci_committed()
            temp_rci = dl_req.temp_rci
            return web.json_response(
                {
                    "plain_headers": [[k, v] for k, v in temp_rci.plain_headers.items()],
                    "secret_headers": [[k, v] for k, v in temp_rci.secret_headers.items()],
                }
            )

    app.router.add_route("*", "/test", TestView)
    client = await aiohttp_client(app)  # type: ignore[operator]

    resp = await client.get(
        "/test",
        headers=CIMultiDict(
            (
                ("X-Custom-Plain", "plain_val"),
                ("X-Custom-Secret", "secret_val"),
                ("X-Ignore-Me", "ignored"),
                ("Host", "127.0.0.1"),
                ("X-Request-Id", "reqid1"),
            )
        ),
        skip_auto_headers=("User-Agent",),
    )
    assert resp.status == 200
    data = await resp.json()

    plain_keys = [k.lower() for k, _ in data["plain_headers"]]
    assert "x-custom-plain" in plain_keys
    assert "host" in plain_keys
    assert "x-ignore-me" not in plain_keys

    secret_keys = [k.lower() for k, _ in data["secret_headers"]]
    assert "x-custom-secret" in secret_keys
    assert "x-ignore-me" not in secret_keys


@pytest.mark.asyncio
async def test_rci_headers_with_commit_rci(aiohttp_client: TestClient) -> None:
    app = web.Application(
        middlewares=[
            dl_api_commons_aio_middlewares.RequestBootstrap(
                req_id_service=dl_api_commons_aio_middlewares.RequestId(),
            ).middleware,
            dl_api_commons_aio_middlewares.rci_headers_middleware(
                rci_extra_plain_headers=("X-Custom-Plain",),
                rci_extra_secret_headers=("X-Custom-Secret",),
            ),
            dl_api_commons_aio_middlewares.commit_rci_middleware(
                rci_extra_plain_headers=("X-Custom-Plain",),
                rci_extra_secret_headers=("X-Custom-Secret",),
            ),
        ]
    )

    class TestView(DLRequestView):
        async def get(self) -> web.Response:
            rci = self.dl_request.rci
            return web.json_response(
                {
                    "plain_headers": [[k, v] for k, v in rci.plain_headers.items()],
                    "secret_headers": [[k, v] for k, v in rci.secret_headers.items()],
                    "req_id": rci.request_id,
                }
            )

    app.router.add_route("*", "/test", TestView)
    client = await aiohttp_client(app)  # type: ignore[operator]

    resp = await client.get(
        "/test",
        headers=CIMultiDict(
            (
                ("X-Custom-Plain", "plain_val"),
                ("X-Custom-Secret", "secret_val"),
                ("X-Ignore-Me", "ignored"),
                ("X-Request-Id", "reqid1"),
                ("Host", "127.0.0.1"),
                ("Accept-Language", "en"),
            )
        ),
        skip_auto_headers=("User-Agent",),
    )
    assert resp.status == 200
    data = await resp.json()
    assert data["req_id"] == "reqid1"

    plain_keys = [k.lower() for k, _ in data["plain_headers"]]
    assert "x-custom-plain" in plain_keys
    assert "host" in plain_keys
    assert "accept-language" in plain_keys
    assert "x-ignore-me" not in plain_keys

    secret_keys = [k.lower() for k, _ in data["secret_headers"]]
    assert "x-custom-secret" in secret_keys
