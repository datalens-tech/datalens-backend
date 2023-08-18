"""
Potentially-reusable somewhat-Quart-specific utils.
"""

from __future__ import annotations

import sys
import time
import logging
import random
import functools
import traceback

from bi_utils.aio import alist

from ..utils import hostname_short, maybe_postmortem

LOGGER = logging.getLogger(__name__)
REQLOGGER = LOGGER.getChild('req')


def _demodel(value):
    """ Convert `openapi_core` model instances to dicts, recursively """
    from openapi_core.extensions.models.models import BaseModel

    result = value
    if isinstance(result, BaseModel):
        result = vars(result)

    if isinstance(result, dict):  # dict / was-a-Model
        return {
            subkey: _demodel(subvalue)
            for subkey, subvalue in result.items()}
    if isinstance(result, (list, tuple)):
        return [
            _demodel(subvalue)
            for subvalue in result]
    return result


def validate_data(request, body, path_pattern=None, api_prefix=''):
    from . import validation
    mock_request = validation.make_mock_request(
        request=request, body=body,
        host_url=api_prefix,
        path_pattern=f'{api_prefix}{path_pattern}' if path_pattern else '',
    )
    try:
        result = validation.validate(mock_request)
    except validation.OpenAPIError as exc:
        return {}, [exc.args[0]]
    data = _demodel(result.body)
    # Also useful: result.parameters (query and header params gathered by schema)
    errors = [error.args[0] for error in result.errors]
    return data, errors


def log_response_body(body, case='...', status=None, request=None):
    if request is not None:
        context = getattr(request, '_verbose_context', {})
        context['response_body_info'] = dict(case=case, body=body, status=status)
    else:
        LOGGER.debug("Response body (%s): %r", case, body)


def db_retry_wrap(func, max_tries=3):
    from .. import db_utils

    @functools.wraps(func)
    async def wrapped_with_db_retries(*args, **kwargs):
        for retries_remain in reversed(range(max_tries)):
            try:
                # XXX: Maybe need to `kwargs['mgr'] = kwargs['mgr'].clone()'
                return await func(*args, **kwargs)
            except Exception as exc:  # pylint: disable=broad-except
                if not retries_remain:
                    raise
                if not db_utils.can_retry_error(exc):
                    raise
                setattr(exc, '_already_retried', True)
                LOGGER.warning("db error (retriable %r): %r", retries_remain, exc)
        raise Exception("Programming Error")

    return wrapped_with_db_retries


@db_retry_wrap
async def simple_db_read(mgr, *args, **kwargs):
    mgr = mgr.clone()
    async with mgr.db.manage(writing=False, tx=False):
        results = await mgr.db_conn.execute(*args, **kwargs)
        results = await alist(results)
    return results


# ### Hooks ###

def req_in(request):
    setattr(request, '_verbose_context', {})
    setattr(request, '_start_time', time.monotonic())
    setattr(request, '_start_time_ts', time.time())


def ensure_request_id(request, tag):
    request_id = getattr(request, 'request_id', None)
    if request_id is not None:
        return

    ts = getattr(request, '_start_time_ts', None) or time.time()
    ts = int(ts * 1000)
    rnd = random.getrandbits(6 * 4)
    host = hostname_short()
    request_id = '%s.%s.%011x.%06x' % (
        tag, host, ts, rnd)

    parent = request.headers.get('X-Request-Id')
    if parent:
        request_id = '{}-{}'.format(parent, request_id)

    setattr(request, 'request_id', request_id)


def sanitize_headers(headers):
    if hasattr(headers, 'items'):
        headers = headers.items()
    headers = {
        key.lower().replace('_', '-'): value
        for key, value in headers}
    for key in ('x-api-key', 'x-yacloud-subjecttoken', 'authorization'):
        value = headers.get(key)
        if value is not None:
            value = '{}***{}'.format(value[:2], value[-2:])
            headers[key] = value
    return headers


def req_info(request, app=None):
    """ Minmal request information for logging """
    result = dict(
        event_code='http_response',
        # request_scheme=request.scheme,
        request_method=request.method,
        request_path=request.full_path,
        # request_path=request.path,
        # request_args=groupby(request.args.items()),
        # # Drop the original case for easier filterability.
        request_headers=sanitize_headers(request.headers),
        # request_cookies=request.cookies,
        endpoint_pattern=request.url_rule.rule if request.url_rule else None,
    )
    try:
        app_view_name = app.view_functions[request.url_rule.endpoint].__name__
    except Exception:
        app_view_name = None
    result['endpoint_code'] = app_view_name
    return result


def get_request_id(request):
    try:
        return getattr(request, 'request_id', '?')
    except RuntimeError:
        return '-'


def request_to_log_str(request):
    """
    Small human-readable representation of the request, for logging.
    """
    return '%s %s' % (request.method, request.path)


def log_req_in(request, app=None):
    REQLOGGER.debug(
        "Handling request request_id=%r: (recv) %s",
        get_request_id(request=request),
        request_to_log_str(request=request),
        extra=req_info(request=request, app=app))


def add_response_request_id(response, request):
    response.headers['X-Request-Id'] = get_request_id(request=request)
    return response


def log_req_out(response, request, app=None):
    start_time = getattr(request, '_start_time', float('nan'))
    td = time.monotonic() - start_time
    td = round(td, 4)
    context = getattr(request, '_verbose_context', None) or {}
    context = context.copy()
    response_body_info = context.pop('response_body_info', None)
    info = dict(
        req_info(request=request, app=app),
        response_status=response.status_code,
        response_timing=td,
        response_body_info=response_body_info,
        response_details=context,
    )
    REQLOGGER.info(
        "Finished request request_id=%r: %s %s",
        get_request_id(request=request),
        response.status_code,
        request_to_log_str(request=request),
        extra=info)
    return response


def on_exception(exc, request, app, jsonify=None):
    if jsonify is None:
        import quart
        jsonify = quart.jsonify
    ei = sys.exc_info()
    REQLOGGER.exception("Request %r error: %r", get_request_id(request=request), exc)
    maybe_postmortem(ei, print_exc=False)
    tb = traceback.format_exception(*ei)
    body = dict(exc_repr=repr(exc), tb=tb)
    log_response_body(body, case='500 Error', status=500)
    response = jsonify(body)
    response.status_code = 500
    log_req_out(response, request=request, app=app)
    return response


def setup_app(app, add_on_exc=True, tag='dls'):
    from quart import request as ctx_request
    app.before_request(functools.partial(req_in, request=ctx_request))
    app.before_request(functools.partial(ensure_request_id, request=ctx_request, tag=tag))
    app.before_request(functools.partial(log_req_in, request=ctx_request, app=app))
    app.after_request(functools.partial(add_response_request_id, request=ctx_request))
    app.after_request(functools.partial(log_req_out, request=ctx_request, app=app))
    if add_on_exc:
        # app.errorhandler(Exception)(...)  # - not quite what's needed.
        app.errorhandler(500)(functools.partial(on_exception, request=ctx_request, app=app))
    return app
