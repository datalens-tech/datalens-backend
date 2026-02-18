from aiohttp import web
from aiohttp.test_utils import TestClient
from multidict import CIMultiDict
import pytest

import dl_api_commons.aio.middlewares as dl_api_commons_aio_middlewares
from dl_api_commons.aio.middlewares.commons import DLRequestBase
from dl_api_commons.aiohttp.aiohttp_wrappers import (
    DLRequestView,
    RCINotSet,
)


@pytest.mark.parametrize("rci_commit", [True, False])
@pytest.mark.asyncio
async def test_rci_headers_populates_rci(aiohttp_client: TestClient, rci_commit: bool) -> None:
    middlewares = [
        dl_api_commons_aio_middlewares.RequestBootstrap(
            req_id_service=dl_api_commons_aio_middlewares.RequestId(),
        ).middleware,
        dl_api_commons_aio_middlewares.rci_headers_middleware(
            rci_extra_plain_headers=("X-Custom-Plain",),
            rci_extra_secret_headers=("X-Custom-Secret",),
        ),
    ]
    if rci_commit:
        middlewares.append(dl_api_commons_aio_middlewares.commit_rci_middleware())

    app = web.Application(middlewares=middlewares)

    class TestView(DLRequestView[DLRequestBase]):
        async def get(self) -> web.Response:
            dl_req = self.dl_request
            if rci_commit:
                rci = dl_req.rci
            else:
                assert not dl_req.is_rci_committed()
                with pytest.raises(RCINotSet):
                    rci = dl_req.rci
                rci = dl_req.temp_rci
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
                ("X-Request-ID", "tests-req-id"),
                ("X-Custom-Plain", "plain_value"),
                ("X-Custom-Secret", "secret_value"),
                ("referer", "http://example.com"),
                ("X-Ignore-Me", "ignored"),
            )
        ),
    )
    assert resp.status == 200
    data = await resp.json()
    assert data["req_id"] == "tests-req-id"

    plain_keys = [k.lower() for k, _ in data["plain_headers"]]
    assert "x-custom-plain" in plain_keys
    assert "x-ignore-me" not in plain_keys
    assert "referer" in plain_keys

    secret_keys = [k.lower() for k, _ in data["secret_headers"]]
    assert "x-custom-secret" in secret_keys
