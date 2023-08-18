"""
Base Quart app, and other stuff to build views on.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import logging
import os
import signal
import time
from typing import TYPE_CHECKING, Optional

from async_timeout import timeout
import attr
from quart import (
    # make_response, render_template, url_for,
    Quart,
    request as ctx_request,
    Response,
    jsonify,
    json,
)
import ujson

from bi_utils.aio import alist
from bi_utils.sanitize import param_bool

from yadls import db
from yadls.exceptions import UserError
from yadls.httpjrpc import qutils
from yadls.httpjrpc.qutils import (
    db_retry_wrap, get_request_id, log_response_body, validate_data,
)
from yadls.manager_aiopg import DLSPG
from yadls.manager_aiopg_cloud import DLSPGC
from yadls.manager_base import NotFound
from yadls.settings import settings

if TYPE_CHECKING:
    import aiopg.sa


HERE = os.path.dirname(__file__)
LOGGER = logging.getLogger(__name__)
API_PREFIX = settings.API_PREFIX

app = Quart(__name__)

BODY_TIMEOUT = 5.0
app.config['BODY_TIMEOUT'] = BODY_TIMEOUT
HANDLER_TIMEOUT = 20.0


@attr.s(auto_attribs=True)
class StateHolder:
    cmstack: Optional[contextlib.AsyncExitStack] = None
    db_engine: Optional[aiopg.sa.engine.Engine] = None
    db_pool: Optional[aiopg.sa.engine.Engine] = None
    db_engine_slave: Optional[aiopg.sa.engine.Engine] = None
    db_pool_slave: Optional[aiopg.sa.engine.Engine] = None
    mgr: Optional[DLSPG] = None
    selfcheck_task: Optional[asyncio.Task] = None


STATE = StateHolder()

SHUTDOWN_EVENT = asyncio.Event()
SHUTDOWN_EVENT_SUPPORTED = False


# ### app setup ###

class JSONEncoder(json.JSONEncoder):

    def default(self, object_):  # pylint: disable=method-hidden
        repr_keys = getattr(object_, '_repr_keys', None)
        if isinstance(repr_keys, tuple):
            result = {key: getattr(object_, key, None) for key in repr_keys}
            result['__class__'] = object_.__class__.__name__
            return result
        # return super().default(obj)
        return str(object_)


app.json_encoder = JSONEncoder


def request_id_logmutator(record):
    request_id = get_request_id(request=ctx_request)
    setattr(record, 'request_id', request_id)


@app.before_serving
def init_logging(extra_debugs=()):
    from ..runlib import init_logging as init_logging_base

    init_logging_base(
        extra_mutators=dict(request_id=request_id_logmutator),
        extra_debugs=extra_debugs,
        app_name='dlshttp',
    )

    # Make a cached logger without the plain-stderr handler
    # https://gitlab.com/pgjones/quart/blob/9aeb0e98ab09081677582460f9a45806f98f5b/src/quart/logging.py#L20-24
    # https://gitlab.com/pgjones/quart/blob/9aeb0e98ab09081677582460f9a45806f98f5b/src/quart/app.py#L302
    # https://st.yandex-team.ru/BI-1091
    app._logger = logging.getLogger('quart.app')


def _on_connect_timeout(**kwargs):
    if os.path.exists('/tmp/.dls_allow_db_conntimeout_suicide.flag'):
        # Cause a graceful shutdown.
        # Requires gunicorn, so that it rotates the worker.
        pid = os.getpid()
        LOGGER.error('connect timeout, SIGTERMing pid %r', pid)
        os.kill(pid, signal.SIGTERM)


@app.before_serving
async def state_prepare():
    LOGGER.debug("app.before_serving -> state_prepare")
    cmstack = contextlib.AsyncExitStack()
    STATE.cmstack = cmstack

    STATE.db_engine = await db.get_engine_aiopg()
    STATE.db_pool = await cmstack.enter_async_context(STATE.db_engine)

    STATE.db_engine_slave = await db.get_engine_aiopg(cfg=db.get_cfg_slave())
    STATE.db_pool_slave = await cmstack.enter_async_context(STATE.db_engine_slave)

    mgr_cls = DLSPGC if settings.USE_CLOUD_SUGGEST else DLSPG
    mgr = mgr_cls(db_cfg=dict(
        db_pool_writing=STATE.db_pool,
        db_pool_reading=STATE.db_pool_slave,
        connect_timeout=3,
        # # For experimenting:
        # connect_timeout=0.0004,
        connect_timeout_callback=_on_connect_timeout,
    ))
    STATE.mgr = mgr


class SelfcheckWorker:

    interval = 6.66
    logger = LOGGER.getChild('selfcheck')

    async def send_status(self, is_ok, desc):
        import aiohttp
        import json as base_json
        import socket
        from statcommons.juggler import make_event_to_juggler_request, process_juggler_response

        LOGGER.debug('self-check status: is_ok=%r, desc=%r', is_ok, desc)

        if settings.SOLOMON_SERVICE and settings.SOLOMON_OAUTH:
            async with aiohttp.ClientSession() as session:
                # Making URL here for better logs. Values should not need quoting.
                solomon_url_full = (
                    f'{settings.SOLOMON_HOST}/api/v2/push?'
                    f'project={settings.SOLOMON_PROJECT}&'
                    f'cluster={settings.SOLOMON_CLUSTER}&'
                    f'service={settings.SOLOMON_SERVICE}'
                )
                solomon_data = {
                    "commonLabels": {
                        "env_name": settings.ENV_NAME,
                        "host": socket.gethostname(),
                    },
                    "metrics": [
                        {
                            "name": "selfcheck",
                            "labels": {"pid": str(os.getpid())},
                            "ts": time.time(),
                            "value": 1.0 if is_ok else 0.0,
                        },
                    ],
                }
                req = session.post(
                    url=solomon_url_full,
                    headers={
                        'Authorization': f'OAuth {settings.SOLOMON_OAUTH}',
                    },
                    json=solomon_data,
                    timeout=7,
                )
                async with req as resp:
                    resp_data = await resp.json()
                    LOGGER.debug(
                        'Solomon response: %r %r -> %r',
                        solomon_url_full, solomon_data, resp_data)

        # Probably not useful, but leaving it here just in case:
        request = make_event_to_juggler_request(
            service='dl-dls-backend-selfcheck',
            status='OK' if is_ok else 'WARN',
            description=base_json.dumps(desc),
            host=settings.ENV_NAME,
            tags=['h_{}_p_{}'.format(socket.gethostname(), os.getpid())],
        )
        async with aiohttp.ClientSession() as session:
            req = session.post(
                url=request['url'],
                json=request['body_data'],
                timeout=7,
            )
            async with req as resp:
                resp.raise_for_status()
                resp_data = await resp.json()
        process_juggler_response(resp_data)

    async def run_once(self):
        db_pool = STATE.db_pool
        if db_pool is None:
            self.logger.error('No `db_pool` in `STATE`')
            return
        t01 = time.monotonic()
        desc: dict[str, object]
        try:
            async with timeout(HANDLER_TIMEOUT):
                async with db_pool.acquire() as conn:
                    result = await conn.execute('select 1 as a')
                    result = await alist(result)
                    assert result
        except Exception as exc:
            t02 = time.monotonic()
            is_ok = False
            desc = dict(error=repr(exc))
        else:
            t02 = time.monotonic()
            is_ok = True
            desc = dict()
        desc.update(is_ok=is_ok, duration=round(t02 - t01, 3))
        return await self.send_status(is_ok=is_ok, desc=desc)

    async def try_run_once(self):
        try:
            return await self.run_once()
        except Exception as exc:
            self.logger.exception('SelfcheckWorker run_once internal error: %r', exc)
            return None

    async def run_forever(self):
        while True:
            await self.try_run_once()
            await asyncio.sleep(self.interval)


SELFCHECK_WORKER = SelfcheckWorker()


@app.before_serving
async def run_selfcheck_task():
    assert STATE.selfcheck_task is None
    task = asyncio.get_event_loop().create_task(SELFCHECK_WORKER.run_forever())
    STATE.selfcheck_task = task


@app.after_serving
async def stop_selfcheck_task():
    task = STATE.selfcheck_task
    if task is not None:
        task.cancel()
        await task


@app.after_serving
async def state_close():
    LOGGER.debug("app.after_serving -> state_close")
    cmstack = STATE.cmstack
    STATE.mgr = None
    STATE.db_pool_slave = None
    STATE.db_engine_slave = None
    STATE.db_pool = None
    STATE.db_engine = None
    STATE.cmstack = None
    if cmstack is not None:
        await cmstack.__aexit__(None, None, None)


qutils.setup_app(app)


# ### commons ###

def common_wrap(schema_path):
    # if route_path is None:
    #     # '/path/{param}' -> '/path/<param>'
    #     route_path = schema_path.replace('{', '<').replace('}', '>')

    def wrap_with_schema_specific(view_func):

        @functools.wraps(view_func)
        async def wrapped_with_schema(*args, **kwargs):
            request = ctx_request
            context = getattr(request, '_verbose_context', {})

            api_key = request.headers.get('X-API-Key')
            if api_key is None:
                response = jsonify(dict(message="Missing required header: `X-API-Key`"))
                response.status_code = 403
                return response
            if api_key != settings.API_KEY:
                response = jsonify(dict(message="Incorrect value in `X-API-Key`"))
                response.status_code = 403
                return response

            data = body = None
            should_validate = not request.args.get('_disable_validation')
            if request.headers.get('Content-Length') or request.headers.get('Transfer-Encoding'):
                # async with timeout(BODY_TIMEOUT):
                body = await request.get_data()
                # # Or:
                # data = await request.get_json()

            body_size = len(body) if body is not None else None
            context.update(request_body_size=body_size)
            if body_size:
                assert isinstance(body, (bytes, str))
                LOGGER.debug("DLS request body: %d bytes: %r", body_size, body[:16384])

            if should_validate and body is not None:
                data, errors = validate_data(
                    request=request, body=body, path_pattern=schema_path,
                    api_prefix=API_PREFIX,
                )
                if errors:
                    response = jsonify(dict(detail=errors))
                    response.status_code = 400
                    return response
            else:
                if body:
                    data = ujson.loads(body.decode('utf-8'))  # type: ignore # pylint: disable=c-extension-no-member
                else:
                    data = None

            t1 = time.time()
            t2 = t1

            handler_timeout = HANDLER_TIMEOUT
            handler_timeout_override = request.args.get('_override_handler_timeout')
            if handler_timeout_override:
                try:
                    handler_timeout = float(handler_timeout_override)
                except Exception:
                    pass

            try:
                mgr = STATE.mgr
                async with contextlib.AsyncExitStack() as handler_cmstack:
                    async with timeout(handler_timeout):
                        response = await view_func(*args, request=request, data=data, mgr=mgr, handler_cmstack=handler_cmstack, **kwargs)
            except NotFound as exc:
                response_body = dict(message=exc.args[0])
                log_response_body(response_body, case='404 NotFound', status=404, request=request)
                response = jsonify(response_body)
                response.status_code = 404
            except UserError as exc:
                response_body = exc.detail
                log_response_body(response_body, case='{} UserError'.format(exc.status_code), status=exc.status_code, request=request)
                response = jsonify(exc.detail)
                response.status_code = exc.status_code
            if not isinstance(response, (str, Response)):
                response_body = response
                log_response_body(response_body, case='200, JSON', status=200, request=request)
                response = jsonify(response_body)
            t3 = time.time()
            # LOGGER.debug(
            #     "DLS timing: func=%r, status=%r, pre=%.3f, handle=%.3f",
            #     view_func.__name__, response.status_code, t2 - t1, t3 - t2)
            context['inner_timing'] = dict(
                func=view_func.__name__, status=response.status_code,
                pre=t2 - t1, handle=t3 - t2)

            return response

        return wrapped_with_schema

    return wrap_with_schema_specific


def get_tenant_info(request, require: bool = True) -> dict[str, str]:
    result: dict[str, str] = {}

    # DLS-specific tenant; primarily for the integrational tests.
    realm = request.args.get('realm') or request.args.get('scope')
    if realm:
        result.update(realm=realm)

    folder_id = request.headers.get("X-YaCloud-FolderId") or request.args.get('folderid')
    if folder_id:
        result.update(folder_id=folder_id)

    cloud_id = request.headers.get("X-YaCloud-CloudId")
    if cloud_id:
        result.update(cloud_id=cloud_id)

    tenant_id = request.headers.get("X-DL-TenantId")
    if tenant_id:
        pfx = "org_"
        if tenant_id.startswith(pfx):
            result.update(org_id=tenant_id.removeprefix(pfx))
        else:
            result.update(folder_id=tenant_id)

    if require and not result:
        raise Exception("Must specify `X-DL-TenantId` or `X-YaCloud-CloudId` or `X-YaCloud-FolderId`")

    return result


def manager_manage(**manage_kwargs):

    def wrap_with_manager_manage(func):

        @functools.wraps(func)
        async def wrapped_with_manager_manage(*args, mgr=None, **kwargs):
            request = ctx_request
            # Ensure the func gets own connections from the pool,
            # TODO?: local-async-context db manager.
            mgr = mgr.clone()

            mgr.context['request_id'] = get_request_id(request)
            mgr.context['iam_token'] = request.headers.get('X-YaCloud-SubjectToken')
            mgr.context['tenant_info'] = get_tenant_info(request, require=False)

            # “sudo”: return error if user doesn't have the superuser group; implies “allow_superuser”.
            mgr.context['sudo'] = param_bool(request.headers.get('X-DL-Sudo'))
            # See `check_permission_by_perm_kinds_base`'s `with_superuser` flag.
            # For transition purposes, defaults to 'true'
            mgr.context['allow_superuser'] = param_bool(request.headers.get('X-DL-Allow-Superuser', 'false'))

            writing_override = request.args.get('_force_db_master')
            if writing_override:
                manage_kwargs['writing'] = True

            async with mgr.db.manage(**manage_kwargs):
                return await func(*args, mgr=mgr, **kwargs)

        return wrapped_with_manager_manage

    return wrap_with_manager_manage


def common_common_wrap(schema_path, writing=False, tx=False):

    def wrap_with_common_common_wrap(func):
        func = manager_manage(writing=writing, tx=tx)(func)
        func = db_retry_wrap(func)
        func = common_wrap(schema_path=schema_path)(func)
        return func

    return wrap_with_common_common_wrap


def get_userid(request, uid=None):
    if uid is None:
        uid = request.headers.get('X-User-Id')
        if not uid:
            raise Exception("Header `X-User-Id` is required here")
    return uid
