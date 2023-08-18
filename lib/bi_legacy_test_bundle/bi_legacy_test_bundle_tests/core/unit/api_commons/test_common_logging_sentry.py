from __future__ import annotations

import asyncio
import itertools
import logging
import time
import uuid
from collections import defaultdict
from multiprocessing import Process
from typing import Any

import attr
import pytest
import sentry_sdk
from aiohttp import web, ClientSession, ClientConnectorError

from bi_constants.api_constants import DLHeadersCommon
from bi_api_commons.logging_sentry import cleanup_common_secret_data
from bi_core.flask_utils.sentry import configure_raven_for_flask
from bi_testing.api_wrappers import HTTPClientWrapper


@attr.s
class PotentiallySensitiveObject:
    yc_session: Any = attr.ib()


@attr.s
class NonSecretObject:
    a: Any = attr.ib()


@attr.s
class CheckSessionRequest:
    pass


S3_TBL_FUNC = '''select * from s3(
    'http://s3-storage/bucket/file.native',
    'access_key_id',
    'secret_access_key',
    'Native',
    'c1 String, c2 Int64'
) as t1
join (select c3, c4 from s3(
    'http://s3-storage/bucket/file.native',
    'access_key_id',
    'secret_access_key',
    'Native',
    'c3 String, c2 Int64'
)) as t2 on t1.c2 = t2.c4
'''


def subproc_func(sentry_url: str):
    def nested_err_func(some_secret):
        generated_password = some_secret + "gg"  # noqa
        raise ValueError("Error")

    def err_func(fernet_key):
        token = "token_" + fernet_key
        PWD = "some_pwd"  # noqa: F841
        my_pwd_old = 'sdsd'  # noqa: F841
        just_a_regular_var = (  # noqa: F841
            "-----BEGIN PRIVATE KEY-----\nasdfasdfas\n-----END PRIVATE KEY-----"
        )
        some_object = PotentiallySensitiveObject(yc_session='cookie_value')  # noqa
        some_other_object = NonSecretObject(a='asdad')  # noqa
        check_session_request = CheckSessionRequest()  # noqa
        s3_query_with_keys = S3_TBL_FUNC  # noqa
        nested_err_func(token)

    log = logging.getLogger()
    sentry_sdk.init(
        sentry_url,
        before_send=cleanup_common_secret_data,
    )

    try:
        err_func("my_fernet-key")
    except Exception:
        log.exception("Some exc")

    sentry_sdk.flush()


SECRET_VAR_NAMES = [
    'token',
    'fernet_key',
    'some_secret',
    'generated_password',
    'my_pwd_old',
    'PWD',
    'just_a_regular_var',  # by var content
    'some_object',  # by var content
    'check_session_request',  # by var content
]


S3_TBL_FUNC_MASKED = '''select * from s3(<hidden>
    'Native',
    'c1 String, c2 Int64'
) as t1
join (select c3, c4 from s3(<hidden>
    'Native',
    'c3 String, c2 Int64'
)) as t2 on t1.c2 = t2.c4
'''


PARTIALLY_MASKED_VARS = {
    's3_query_with_keys': {repr(S3_TBL_FUNC_MASKED)},
}


@attr.s
class SentryMock:
    dsn: str = attr.ib()
    rq_json_store: list[dict[str, Any]] = attr.ib()
    got_data_event: asyncio.Event = attr.ib()

    next_event_idx: int = attr.ib(init=False, default=0)

    async def get_next_event(self) -> dict[str, Any]:
        # Non-concurrency safe!!!
        while True:
            if len(self.rq_json_store) > self.next_event_idx:
                return self.rq_json_store[self.next_event_idx]

            await asyncio.wait_for(self.got_data_event.wait(), timeout=3.)


@pytest.fixture(scope='function')
async def sentry_mock(loop, aiohttp_client) -> SentryMock:
    sentry_spy = web.Application()
    rq_json_store = []
    got_data_event = asyncio.Event()

    async def sentry_spy_view(request: web.Request):
        rq_json_store.append(await request.json())
        got_data_event.set()
        got_data_event.clear()
        return web.Response(status=200)

    sentry_spy.router.add_route('*', '/{tail:.*}', sentry_spy_view)
    test_client = await aiohttp_client(sentry_spy)

    sentry_url = f"http://{uuid.uuid4().hex}@{test_client.host}:{test_client.port}/100500"

    return SentryMock(dsn=sentry_url, rq_json_store=rq_json_store, got_data_event=got_data_event)


def run_aiohttp_server(sentry_dsn: str, host: str, port: int):
    """
    Function that constructs and launch test aiohttp app with configured Sentry client.
    Must be launched in subprocess!
    """
    asyncio.set_event_loop(None)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    import sentry_sdk
    from sentry_sdk.integrations.aiohttp import AioHttpIntegration
    from sentry_sdk.integrations.atexit import AtexitIntegration
    from sentry_sdk.integrations.excepthook import ExcepthookIntegration
    from sentry_sdk.integrations.stdlib import StdlibIntegration
    from sentry_sdk.integrations.modules import ModulesIntegration
    from sentry_sdk.integrations.argv import ArgvIntegration
    from sentry_sdk.integrations.logging import LoggingIntegration
    from sentry_sdk.integrations.threading import ThreadingIntegration

    # from sentry_sdk.integrations.logging import LoggingIntegration
    sentry_sdk.init(
        dsn=sentry_dsn,
        default_integrations=False,
        before_send=cleanup_common_secret_data,
        integrations=[
            # # Default
            AtexitIntegration(),
            ExcepthookIntegration(),
            StdlibIntegration(),
            ModulesIntegration(),
            ArgvIntegration(),
            LoggingIntegration(event_level=logging.WARNING),
            ThreadingIntegration(),
            #  # Custom
            AioHttpIntegration(),
        ],
    )

    app = web.Application()

    async def fail_on_any(_: web.Request):
        secret_var = 123  # noqa
        raise ValueError("What did you expect?")

    async def ping(_: web.Request):
        return web.json_response({}, status=200)

    async def the_ok_but_error_in_logs(_: web.Request):
        secret_var = "123"
        try:
            raise ValueError()
        except ValueError:
            logging.exception("Got exception")
        return web.json_response({"secret": secret_var}, status=200)

    app.router.add_route('GET', '/ping', ping)
    app.router.add_route('*', '/the_exception', fail_on_any)
    app.router.add_route('*', '/the_ok_but_error_in_logs', the_ok_but_error_in_logs)

    web.run_app(app, host=host, port=port)


def run_flask_server(sentry_dsn: str, host: str, port: int):
    """
    Function that constructs and launch test flask app with configured Sentry client.
    Must be launched in subprocess!
    """
    import flask
    from werkzeug.serving import make_server
    from bi_core.logging_config import configure_logging

    app = flask.Flask(__name__)
    configure_raven_for_flask(app, sentry_dsn=sentry_dsn)
    configure_logging(
        "flask_raven_sentry_client",
        app_prefix="tst_1",
        sentry_dsn=sentry_dsn,
        # Default is `development`. Sentry logging integration will not be configured if we leave it.
        env="NotADevelopment",
    )

    def fail_on_any():
        secret_var = 123  # noqa
        raise ValueError("What did you expect?")
        # return flask.jsonify({})

    def ping():
        return flask.jsonify({})

    def the_ok_but_error_in_logs():
        secret_var = "123"
        try:
            raise ValueError()
        except ValueError:
            logging.getLogger("bi_core.us_connection").exception("Got exception")
        return flask.jsonify({"secret": secret_var})

    app.add_url_rule("/ping", view_func=ping)
    app.add_url_rule("/the_exception", view_func=fail_on_any)
    app.add_url_rule("/the_ok_but_error_in_logs", view_func=the_ok_but_error_in_logs)

    server = make_server(
        app=app,
        host=host,
        port=port,
    )
    server.serve_forever()


@attr.s(auto_attribs=True)
class SentryClientWebApp:
    client: HTTPClientWrapper
    proc: Process


# @pytest.mark.skip('Tends to timeout. Needs fixing')
@pytest.mark.asyncio
async def test_locals_cleanup(loop, sentry_mock: SentryMock):
    sentry_url = sentry_mock.dsn
    rq_json_store = sentry_mock.rq_json_store

    sentry_client_proc = Process(target=subproc_func, args=(sentry_url,))

    await loop.run_in_executor(None, sentry_client_proc.start)
    await asyncio.wait_for(
        loop.run_in_executor(None, sentry_client_proc.join),
        timeout=10,
    )

    assert len(rq_json_store) == 1

    event = await sentry_mock.get_next_event()

    all_frames = itertools.chain(*[exc['stacktrace']['frames'] for exc in event['exception']['values']])

    collected_secret_var_value_set = defaultdict(set)
    collected_partially_masked_var_value_set = defaultdict(set)
    for frame in all_frames:
        for var_name, var_val in frame['vars'].items():
            if var_name in SECRET_VAR_NAMES:
                collected_secret_var_value_set[var_name].add(var_val)
            elif var_name in PARTIALLY_MASKED_VARS:
                collected_partially_masked_var_value_set[var_name].add(var_val)

    assert dict(collected_secret_var_value_set.items()) == {
        var_name: {'[hidden]'} for var_name in SECRET_VAR_NAMES
    }

    assert dict(collected_partially_masked_var_value_set.items()) == PARTIALLY_MASKED_VARS


@pytest.fixture(scope='function', params=(run_flask_server, run_aiohttp_server))
def sentry_client_web_app(loop, request, unused_tcp_port, sentry_mock: SentryMock) -> SentryClientWebApp:
    host: str = "127.0.0.1"
    port: int = unused_tcp_port

    target_func = request.param

    sentry_client_proc = Process(target=target_func, args=(
        sentry_mock.dsn,
        host,
        port,
    ))
    sentry_client_proc.start()

    session = ClientSession()
    client = HTTPClientWrapper(session=session, base_url=f"http://{host}:{port}/")

    start_time = time.monotonic()
    wait_limit = 5.

    while True:
        if time.monotonic() > start_time + wait_limit:
            raise Exception("Dummy Sentry testing web app subprocess awaiting timeout")
        elif not sentry_client_proc.is_alive():
            raise Exception("Dummy Sentry testing web app subprocess found dead while waiting for up")
        try:
            resp = loop.run_until_complete(client.request("get", "ping"))
            if resp.status == 200:
                break
        except ClientConnectorError:
            pass

        time.sleep(0.01)

    yield SentryClientWebApp(client, sentry_client_proc)

    if sentry_client_proc.is_alive():
        sentry_client_proc.terminate()
        sentry_client_proc.join(timeout=1.)
    if sentry_client_proc.is_alive():
        sentry_client_proc.kill()
        sentry_client_proc.join()


def keys_to_lower(d: dict[str, str]) -> dict[str, str]:
    return {
        h_name.lower(): h_value
        for h_name, h_value in d.items()
    }


@pytest.mark.asyncio
async def test_locals_cleanup_in_apps(sentry_client_web_app: SentryClientWebApp, sentry_mock: SentryMock) -> None:
    resp = await sentry_client_web_app.client.request(
        "get", "the_ok_but_error_in_logs", headers={DLHeadersCommon.IAM_TOKEN.value: "iam6"}
    )
    assert resp.status == 200
    evt = await sentry_mock.get_next_event()
    # We expect only one event
    assert len(sentry_mock.rq_json_store) == 1

    # To be able to connect with debugger
    sentry_client_web_app.proc.terminate()

    # Just raven logging client also applies headers sanitizer
    # Because currently in flask apps there are 2 clients: one for logging, another one for Flask itself
    if "request" in evt:
        evt_iam_token = keys_to_lower(evt['request']['headers'])[DLHeadersCommon.IAM_TOKEN.value.lower()]
        assert evt_iam_token == "<hidden>"

    all_frames = list(itertools.chain(*[exc['stacktrace']['frames'] for exc in evt['exception']['values']]))
    # We throw and capture variable inside of function so we shoud get single frame
    assert len(all_frames) == 1
    frame = all_frames[0]
    # Check that variable was sanitized
    assert frame["vars"]["secret_var"] == "[hidden]"


@pytest.mark.asyncio
async def test_headers_cleanup(sentry_client_web_app: SentryClientWebApp, sentry_mock: SentryMock) -> None:
    req_headers = {
        "some-not-hardcodded-secret-token": "hide_me_please",
        DLHeadersCommon.IAM_TOKEN.value: "hide_me",
        DLHeadersCommon.AUTHORIZATION_TOKEN.value: "NoNo",
        DLHeadersCommon.REQUEST_ID.value: "my_req_id",
    }

    resp = await sentry_client_web_app.client.request(
        "get", "the_exception",
        headers=req_headers
    )
    assert resp.status == 500

    evt = await sentry_mock.get_next_event()

    # To be able to connect with debugger
    sentry_client_web_app.proc.terminate()

    assert 'request' in evt, evt
    evt_headers = evt['request']['headers']

    # Gathering header manually set header from event (filter out Content-Type, Keepalive, etc...)
    relevant_event_headers = {
        h_name: h_val
        for h_name, h_val in evt_headers.items()
        if h_name.lower() in keys_to_lower(req_headers)
    }

    # Check that all sent headers are in event
    assert set(keys_to_lower(relevant_event_headers)) == set(keys_to_lower(req_headers))

    assert keys_to_lower(relevant_event_headers) == keys_to_lower({
        "some-not-hardcodded-secret-token": "hid<hidden>ase",
        DLHeadersCommon.IAM_TOKEN.value: "<hidden>",
        DLHeadersCommon.AUTHORIZATION_TOKEN.value: "<hidden>",
        DLHeadersCommon.REQUEST_ID.value: "my_req_id",
    })
