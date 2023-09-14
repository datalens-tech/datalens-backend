import pytest

from bi_api_commons.client.common import Req
from bi_constants.api_constants import (
    DLHeaders,
    DLHeadersCommon,
)
from bi_file_uploader_api_lib_tests.req_builder import ReqBuilder


class TempHeaders(DLHeaders):
    X_UNAUTHORIZED = "x-unauthorized"


@pytest.mark.asyncio
@pytest.mark.parametrize("view", ["ping", "metrics"])
async def test_basic(fu_client, view):
    resp = await fu_client.make_request(Req("get", f"/api/v2/{view}"))
    assert resp.status == 200


@pytest.mark.asyncio
async def test_cors(fu_client):
    cors_headers = (
        "Access-Control-Expose-Headers",
        "Access-Control-Allow-Origin",
        "Access-Control-Allow-Credentials",
        "Access-Control-Allow-Methods",
    )

    resp = await fu_client.make_request(
        Req(
            method="options",
            url="/api/v2/files",
            extra_headers={
                TempHeaders.X_UNAUTHORIZED: "1",
                DLHeadersCommon.ACCESS_CTRL_REQ_HEADERS: "x-csrf-token,x-request-id",
                DLHeadersCommon.ACCESS_CTRL_REQ_METH: "POST",
                DLHeadersCommon.ORIGIN: "https://foo.bar",
                DLHeadersCommon.REQUEST_ID: "qwerty",
            },
        ),
    )
    assert resp.status == 200
    for header in cors_headers:
        assert header in resp.headers

    resp = await fu_client.make_request(
        Req(
            method="options",
            url="/api/v2/files",
            extra_headers={
                TempHeaders.X_UNAUTHORIZED: "1",
                DLHeadersCommon.ACCESS_CTRL_REQ_HEADERS: "x-csrf-token,x-request-id",
                DLHeadersCommon.ACCESS_CTRL_REQ_METH: "POST",
                DLHeadersCommon.ORIGIN: "https://some.host",  # wrong host
            },
        ),
    )
    assert resp.status == 200
    for header in cors_headers:
        assert header not in resp.headers


@pytest.mark.asyncio
async def test_request_schema_validation_error(fu_client):
    resp = await fu_client.make_request(
        Req(
            method="post",
            url="/api/v2/links",
            data_json={
                "url": "asdf",
            },
            require_ok=False,
        )
    )

    assert resp.status == 400
    assert "Missing data for required field." in resp.json["message"]

    resp = await fu_client.make_request(ReqBuilder.upload_gsheet(url="asdf", authorized=True, require_ok=False))
    assert resp.status == 400
    assert "Either refresh_token or connection_id must be provided when authorized is true" in resp.json["message"]
