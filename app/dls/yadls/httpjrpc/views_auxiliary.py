from __future__ import annotations

import os
import logging
import asyncio
from typing import Any

from async_timeout import timeout

from bi_utils.aio import alist

from .base_app import (
    app,
    API_PREFIX,
    HERE,
    ctx_request,
    jsonify,
    STATE,
)

LOGGER = logging.getLogger(__name__)


@app.route('/docs/', defaults=dict(path=''))
@app.route('/docs/<path:path>')
async def docs_root_redirect(path, request=ctx_request):
    from quart import redirect
    path = request.path
    if request.query_string:
        path = '{}?{}'.format(path, request.query_string)
    # path = request.headers[':Path']  # not always working
    return redirect('{}{}'.format(API_PREFIX, path), code=302)


@app.route('{}/docs/'.format(API_PREFIX), defaults=dict(path='index.html'))
@app.route('{}/docs/<path:path>'.format(API_PREFIX))
async def docs(path):
    if path == 'schemas.yaml':
        docs_root = os.path.join(HERE, '../')
    else:
        docs_root = os.path.join(HERE, 'docs')

    from quart.static import send_from_directory
    return await send_from_directory(docs_root, path)
    # # TMPFIX:
    # from quart.static import send_file, safe_join
    # from quart.exceptions import NotFound
    # file_path = safe_join(docs_root, path)
    # if not os.path.isfile(file_path):
    #     raise NotFound()
    # return await send_file(str(file_path))


ALL_METHODS = ['HEAD', 'OPTIONS', 'GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'HAX']


@app.route('{}/debug/'.format(API_PREFIX), methods=ALL_METHODS, defaults=dict(path=''))
@app.route('{}/debug/<path>'.format(API_PREFIX), methods=ALL_METHODS)
async def debug_view(path, request=ctx_request):
    result: dict[str, Any] = dict(
        request=dict(
            host_url=request.host_url,
            method=request.method,
            path=request.path,
            view_args=request.view_args,
            args=request.args,
            headers=request.headers,
            cookies=request.cookies,
        ),
    )
    LOGGER.info("debug: %r", result)

    request.request_id += '.debug_test'

    body_timeout = float(request.args.get('_body_timeout') or 5.0)

    async with timeout(body_timeout):
        result['request']['body'] = await request.get_data()

    arg_db_sleep = float(request.args.get('_db_sleep') or 0)
    if arg_db_sleep:
        arg_db_level = int(request.args.get('_db_level') or 0)
        mgr_base = STATE.mgr
        assert mgr_base is not None
        mgr = mgr_base.clone()
        async with mgr.db.manage(writing=arg_db_level >= 1, tx=arg_db_level >= 2):
            async with mgr.db.manage(writing=arg_db_level >= 1, tx=arg_db_level >= 2):
                async with mgr.db.manage(writing=arg_db_level >= 1, tx=False):
                    db_res = await mgr.db_conn.execute('select 1, pg_sleep(%s);', arg_db_sleep)
                    db_res = await alist(db_res)
                    result['db_sleep'] = (arg_db_sleep, db_res)  # type: ignore  # TODO: fix
                db_res = await mgr.db_conn.execute('select 2;')
                db_res = await alist(db_res)
                result['db_sleep_p02'] = db_res

    arg_sleep = float(request.args.get('_sleep') or 0)
    if arg_sleep:
        result['sleep'] = (arg_sleep, await asyncio.sleep(arg_sleep))  # type: ignore  # TODO: fix

    arg_raise = request.args.get('_raise')
    if arg_raise:
        raise Exception(arg_raise)

    response = jsonify(result)
    response.status_code = request.args.get('_status_code') or 200

    LOGGER.info("debug: returning: %r / %r", response, result)

    return response
