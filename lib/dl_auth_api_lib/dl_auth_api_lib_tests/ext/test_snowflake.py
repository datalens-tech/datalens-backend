import pytest

from dl_api_commons.client.base import Req
from dl_constants.api_constants import DLHeadersCommon


@pytest.mark.asyncio
async def test_snowflake_token(oauth_app_client, snowflake_payload):
    snowflake_payload["code"] = "1234567"
    resp = await oauth_app_client.make_request(
        Req(
            method="post",
            url="/oauth/token/snowflake",
            data_json=snowflake_payload,
            extra_headers={DLHeadersCommon.ORIGIN: "https://example.com"},
        )
    )
    assert resp.status == 200
    assert "error" in resp.json
    assert resp.json["error"] == "invalid_client"


@pytest.mark.asyncio
async def test_invalid_snowflake_account(oauth_app_client, snowflake_payload):
    snowflake_payload["account"] = "aa12345.eu-central-1"
    snowflake_payload["code"] = "1234567"
    resp = await oauth_app_client.make_request(
        Req(
            method="post",
            url="/oauth/token/snowflake",
            data_json=snowflake_payload,
            extra_headers={DLHeadersCommon.ORIGIN: "https://example.com"},
            require_ok=False,
        )
    )
    assert resp.status == 400
    assert "message" in resp.json
    assert "513" in resp.json["message"]
