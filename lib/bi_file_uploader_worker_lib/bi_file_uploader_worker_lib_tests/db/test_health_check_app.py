import pytest

from bi_testing.api_wrappers import Req


@pytest.mark.asyncio
async def test_basic(health_check_api_client):
    resp = await health_check_api_client.make_request(Req("get", "/ping"))
    assert resp.status == 200
