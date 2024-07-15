import pytest

from dl_api_commons.client.base import Req


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "conn_type, resp_uri",
    [
        (
            "gsheets",
            (
                "https://accounts.google.com/o/oauth2/v2/auth?client_id=gsheets_id&redirect_uri=localhost&"
                "response_type=code&scope=https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fspreadsheets.readonly&"
                "access_type=offline&prompt=consent&include_granted_scopes=true"
            ),
        ),
    ],
)
async def test_google_uri(oauth_app_client, conn_type, resp_uri):
    resp = await oauth_app_client.make_request(
        Req(
            method="get",
            url=f"/oauth/uri/google?conn_type={conn_type}",
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
            url="/oauth/uri/google?type_of_conn=some_conn",
            require_ok=False,
        )
    )
    assert resp.status == 400
