from __future__ import annotations

import logging
import os
from typing import Dict, Any

import flask
import pytest
from aiohttp import web

from multidict import CIMultiDict

from dl_api_commons import clean_secret_data_in_headers, log_request_start, log_request_end
from dl_api_commons.aio.middlewares.request_bootstrap import RequestBootstrap
from dl_api_commons.aio.middlewares import request_id as aio_request_id
from dl_api_commons.logging import mask_sensitive_fields_by_name_in_json_recursive
from dl_api_commons.flask.middlewares.context_var_middleware import ContextVarMiddleware
from dl_api_commons.flask.middlewares.logging_context import RequestLoggingContextControllerMiddleWare
from dl_api_commons.flask.middlewares.request_id import RequestIDService


LOGGER = logging.getLogger(__name__)

_EXPECTED_MASKED_HEADERS = (
    ("SomeToken", "<hidden>"),
    ("some-token", "<hidden>"),
    ("some-tokeN", "<hidden>"),
    ("SomeToken1", "<hidden>"),
    ("SomeToken-1", "<hidden>"),
    ("x-us-master-token", "<hidden>"),
    ("X-US-Master-Token", "<hidden>"),
    ("x-us-master-token", "lon<hidden>890"),
    ("NonSecretHeader", "Ololo"),
    ("NonSecretHeader", "Azaza"),
    ("X-DL-API-Key", "123<hidden>789"),
    ("Cookie", "Session_id=<hidden>; _ym_isad=2; sessionid2=<hidden>"),
    ("Cookie", "_ym_isad=2; session_id=<hidden>; sessionid2=<hidden>"),
)

_INPUT_HEADERS = (
    ("SomeToken", "token"),
    ("some-token", "token"),
    ("some-tokeN", "token"),
    ("SomeToken1", "token"),
    ("SomeToken-1", "token"),
    ("x-us-master-token", "token"),
    ("X-US-Master-Token", "token"),
    ("x-us-master-token", "long67890"),
    ("NonSecretHeader", "Ololo"),
    ("NonSecretHeader", "Azaza"),
    ("X-DL-API-Key", "123456789"),
    ("Cookie", "Session_id=sec_val; _ym_isad=2; sessionid2=sec_val2"),
    ("Cookie", "_ym_isad=2; session_id=sec_valci; sessionid2=sec_val2"),
)


def test_headers_cleaning():
    assert _EXPECTED_MASKED_HEADERS == clean_secret_data_in_headers(_INPUT_HEADERS)


def test_common_logging_request_start(caplog):
    caplog.set_level('INFO')
    caplog.clear()
    log_request_start(
        logger=LOGGER,
        method="get",
        full_path="/api/ololo/azaza?param=value",
        headers=(
            ('Some-Token', "Some_secret_value"),
            ('User-Agent', 'Python-urllib/2.7'),
        )
    )
    assert len(caplog.records) == 1

    rec = caplog.records[0]
    assert rec.name == LOGGER.name
    expected_message = (
        "Received request."
        " method: GET, path: /api/ololo/azaza?param=value,"
        " headers: {'Some-Token': 'Som<hidden>lue', 'User-Agent': 'Python-urllib/2.7'},"
        f" pid: {os.getpid()}"
    )
    assert rec.message == expected_message


def test_common_logging_request_end(caplog):
    caplog.set_level('INFO')
    caplog.clear()
    log_request_end(
        logger=LOGGER,
        method="get",
        full_path="/unistat/?",
        status_code=200,
    )
    assert len(caplog.records) == 1

    rec = caplog.records[0]
    assert rec.name == LOGGER.name
    assert "Response. method: GET, path: /unistat/?, status: 200" == rec.message


request_path = "/?a=b"
request_cookies = {
    "Session_id": "sec_val",
    "sessionid2": "sec_val2",
    "non_secret_cookie": "non_secret_val",
}
request_headers = (
    ("X-YaCloud-SubjectToken", "1234567890"),
    ("Plain-Header", "Plain value"),
    ("User-Agent", "Test client"),
    ("X-Request-Id", "parentreqid1234")
)


# TODO FIX: Request ID tests
def test_common_logging_flask(caplog):
    caplog.set_level('INFO')
    caplog.clear()

    app = flask.Flask(__name__)

    ContextVarMiddleware().wrap_flask_app(app)

    RequestLoggingContextControllerMiddleWare().set_up(app)
    RequestIDService(request_id_app_prefix=None).set_up(app)

    @app.route("/")
    def home():
        return flask.jsonify({"msg": "Hello!"})

    client = app.test_client()
    for c_name, c_val in request_cookies.items():
        client.set_cookie("localhost", c_name, c_val)

    resp = client.get(
        request_path,
        headers={k: v for k, v in request_headers},
    )
    assert resp.status_code == 200
    records = caplog.records

    # FIXME: fix the original stochastic 'Unclosed client session' errors.
    other_records = [rec for rec in records if rec.name == 'asyncio']
    records = [rec for rec in records if rec.name != 'asyncio']
    if other_records:
        LOGGER.error("Extraneous asyncio logrecords: %r", other_records)

    assert len(records) == 2

    expected_start_msg = (
        "Received request. method: GET, path: /?a=b,"
        " headers:"
        " {'User-Agent': 'Test client',"
        " 'Host': 'localhost',"
        " 'X-Yacloud-Subjecttoken': '123<hidden>890',"
        " 'Plain-Header': 'Plain value',"
        " 'X-Request-Id': 'parentreqid1234',"
        " 'Cookie': 'Session_id=<hidden>; non_secret_cookie=non_secret_val; sessionid2=<hidden>'},"
        f" pid: {os.getpid()}"
    )
    expected_end_msg = (
        "Response. method: GET, path: /?a=b, status: 200"
    )

    assert expected_start_msg == records[0].message
    assert expected_end_msg == records[1].message

    # Check that own request id was appended
    internal_request_id = records[0].log_context.get("request_id")
    assert internal_request_id and internal_request_id.startswith("parentreqid1234--")

    parent_request_id = records[0].log_context.get("parent_request_id")
    assert parent_request_id and parent_request_id == 'parentreqid1234'


@pytest.mark.asyncio
async def test_common_logging_aiohttp(caplog, aiohttp_client):
    caplog.set_level('INFO')
    caplog.clear()

    app = web.Application(
        middlewares=[
            RequestBootstrap(
                req_id_service=aio_request_id.RequestId(append_own_req_id=True),
            ).middleware,
        ]
    )

    async def handle(_: web.Request):
        return web.json_response({})

    app.router.add_get("/", handle)

    client = await aiohttp_client(app, cookies=request_cookies)

    resp = await client.get(request_path, headers=CIMultiDict(request_headers))
    assert resp.status == 200

    expected_start_msg = (
        "Received request. method: GET, path: /?a=b,"
        " headers:"
        f" {{'Host': '{client.host}:{client.port}',"
        " 'X-YaCloud-SubjectToken': '123<hidden>890',"
        " 'Plain-Header': 'Plain value',"
        " 'User-Agent': 'Test client',"
        " 'X-Request-Id': 'parentreqid1234',"
        " 'Accept': '*/*',"
        " 'Accept-Encoding': 'gzip, deflate',"
        " 'Cookie': 'Session_id=<hidden>; non_secret_cookie=non_secret_val; sessionid2=<hidden>'},"
        f" pid: {os.getpid()}"
    )
    expected_end_msg = (
        "Response. method: GET, path: /?a=b, status: 200"
    )

    # noqa
    req_id_records = [rec for rec in caplog.records if rec.name == aio_request_id.LOGGER.name]
    assert len(req_id_records) == 2
    assert expected_start_msg == req_id_records[0].message
    assert expected_end_msg == req_id_records[1].message

    # Check that own request id was appended
    internal_request_id = req_id_records[0].log_context.get("request_id")
    assert internal_request_id and internal_request_id.startswith("parentreqid1234--")


@pytest.mark.parametrize('source, expected_masked', (
    (None, None),
    ({}, {}),
    (
        dict(password='1', a=2),
        dict(password='<hidden>', a=2),
    ),
    (
        dict(password=1, token=False, cypher_text=5.0, a=2),
        dict(password='<hidden>', token='<hidden>', cypher_text='<hidden>', a=2),
    ),
    (
        dict(
            a=dict(
                a=dict(
                    token='',
                    password='',
                    cypher_text='<hidden>',
                    sample='asdf'
                )
            )
        ),
        dict(
            a=dict(
                a=dict(
                    token='<hidden>',
                    password='<hidden>',
                    cypher_text='<hidden>',
                    sample='asdf',
                )
            )
        ),
    ),
    (
        dict(items=[dict(password='asdf'), dict(password='asdf')]),
        dict(items=[dict(password='<hidden>'), dict(password='<hidden>')]),
    ),
    (
        dict(password=['asdf', 'fdsa']),
        dict(password=['<hidden>', '<hidden>']),
    ),
))
def test_mask_sensitive_fields_by_name_recursive(source: Dict[str, Any], expected_masked: Dict[str, Any]):
    actual_masked = mask_sensitive_fields_by_name_in_json_recursive(source)
    assert actual_masked == expected_masked
