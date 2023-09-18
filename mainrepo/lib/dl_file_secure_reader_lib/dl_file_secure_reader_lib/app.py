from concurrent.futures import ThreadPoolExecutor
import logging
import os

from aiohttp import web

from dl_file_secure_reader_lib.resources import (
    ping,
    reader,
)

LOGGER = logging.getLogger(__name__)


def create_app() -> web.Application:
    app = web.Application()

    app.router.add_route("*", "/reader/ping", ping.PingView)
    app.router.add_route("*", "/reader/excel", reader.ReaderView)

    app["tpe"] = ThreadPoolExecutor(max_workers=min(32, (os.cpu_count() or 1) * 3 + 4))

    return app
