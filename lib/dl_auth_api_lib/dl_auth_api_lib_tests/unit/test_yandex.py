import urllib.parse

import pytest

from dl_api_commons.client.base import Req
from dl_constants.api_constants import DLHeadersCommon


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "conn_type, resp_uri",
    [
        (
            "metrica",
            "https://oauth.yandex.ru/authorize?client_id=metrica&redirect_uri=localhost&response_type=code&scope=read",
        ),
        (
            "app_metrica",
            "https://oauth.yandex.ru/authorize?client_id=app_metrica&redirect_uri=localhost&response_type=code",
        ),
        (
            "custom_conn",
            "https://oauth.yandex.com/authorize?client_id=custom_conn&redirect_uri=localhost&response_type=code",
        ),
    ],
)
async def test_yandex_uri(oauth_app_client, conn_type, resp_uri):
    resp = await oauth_app_client.make_request(
        Req(
            method="get",
            url=f"/oauth/uri/yandex?conn_type={conn_type}",
        )
    )
    assert resp.status == 200
    assert "uri" in resp.json
    assert resp.json["uri"] == resp_uri


@pytest.mark.asyncio
async def test_request_schema_validation_error(oauth_app_client):
    resp = await oauth_app_client.make_request(
        Req(
            method="get",
            url="/oauth/uri/yandex?type_of_conn=some_conn",
            require_ok=False,
        )
    )
    assert resp.status == 400


@pytest.mark.asyncio
async def test_origin(oauth_app_client):
    origin = "https://example.com"
    resp = await oauth_app_client.make_request(
        Req(
            method="get",
            url="/oauth/uri/yandex?conn_type=metrica",
            extra_headers={DLHeadersCommon.ORIGIN: origin},
        )
    )
    assert resp.status == 200
    assert "uri" in resp.json
    assert urllib.parse.quote_plus(origin) in resp.json["uri"]
