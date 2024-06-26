import pytest

from dl_api_commons.client.base import Req


@pytest.mark.asyncio
async def test_ping(oauth_app_client):
    resp = await oauth_app_client.make_request(
        Req(
            method="get",
            url="/oauth/ping",
        )
    )
    assert resp.status == 200
