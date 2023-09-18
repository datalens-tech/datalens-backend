from __future__ import annotations

import asyncio
import json

from aiohttp import web
from statcommons.unistat.uwsgi import process_uwsgi_data_for_unistat

from dl_api_commons.aiohttp.aiohttp_wrappers import RequiredResourceCommon

from .base import (
    BaseView,
    requires,
)


# TODO FIX: Move to bi-common
async def read_usocket(path):  # type: ignore  # TODO: fix
    """
    Simple "connect to a unix socket at `path` and read the stream".
    """
    reader, writer = await asyncio.open_unix_connection(path=path)
    try:
        return await reader.read()
    finally:
        writer.close()
        await writer.wait_closed()


async def uwsgi_unistat(path):  # type: ignore  # TODO: fix
    data = await read_usocket(path)
    data = json.loads(data)
    return process_uwsgi_data_for_unistat(data)


@requires(RequiredResourceCommon.SKIP_AUTH)
class UnistatView(BaseView):
    async def get(self) -> web.Response:
        result = await uwsgi_unistat("/tmp/uwsgi_stats_rqe_int_sync.sock")
        result = list(result.items())
        return web.json_response(result)
