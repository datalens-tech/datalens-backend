import pytest

from dl_api_commons.client.base import Req


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
