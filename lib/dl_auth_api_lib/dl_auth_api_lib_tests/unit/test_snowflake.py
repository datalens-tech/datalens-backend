import pytest

from dl_api_commons.client.base import Req
from dl_constants.api_constants import DLHeadersCommon


@pytest.mark.asyncio
async def test_snowflake_uri(oauth_app_client, snowflake_payload):
    resp = await oauth_app_client.make_request(
        Req(
            method="get",
            url="/oauth/uri/snowflake",
            params=snowflake_payload,
            extra_headers={DLHeadersCommon.ORIGIN: "https://example.com"},
        )
    )
    assert resp.status == 200
    assert "uri" in resp.json
    assert resp.json["uri"] == (
        "https://gg36894.eu-central-1.snowflakecomputing.com/oauth/authorize?"
        "client_id=snowflake_client_id&redirect_uri=https%3A%2F%2Fexample.com&"
        "response_type=code&state=dl_conn_state"
    )


@pytest.mark.asyncio
async def test_invalid_account_name(oauth_app_client, snowflake_payload):
    snowflake_payload["account"] = "0.0.0.0:443?a="
    resp = await oauth_app_client.make_request(
        Req(
            method="get",
            url="/oauth/uri/snowflake",
            params=snowflake_payload,
            extra_headers={DLHeadersCommon.ORIGIN: "https://example.com"},
            require_ok=False,
        )
    )
    assert resp.status == 400
    assert "Invalid account_name" in resp.json["message"]
