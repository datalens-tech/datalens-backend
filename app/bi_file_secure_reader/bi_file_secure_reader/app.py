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


async def create_gunicorn_app() -> web.Application:
    try:
        LOGGER.info("Creating application instance")
        app = create_app()
        LOGGER.info("Application instance was created")
        return app
    except Exception:
        LOGGER.exception("Exception during app creation")
        raise


def main() -> None:
    current_app = create_gunicorn_app()
    web.run_app(
        current_app,
        host=os.environ["APP_HOST"],
        port=int(os.environ["APP_PORT"]),
    )


if __name__ == "__main__":
    main()
