#!/usr/bin/env python3
'''
Entry point: app on hypercorn.

WARNING: an example; not working; does not support multiple workers.

Docs:
https://pgjones.gitlab.io/hypercorn/api_usage.html

Nearly-equivalent cmd:

    hypercorn \
        --error-log - \
        --workers 4 \
        --bind ":::${DLS_HTTP_PORT:-80}" \
        --uvloop \
        yadls.httpjrpc.main:app
'''

from __future__ import annotations

import asyncio
import os
import signal

import uvloop
from hypercorn.asyncio import serve
from hypercorn.config import Config

from . import base_app
from .main import app


def _signal_handler(*args, **kwargs):
    base_app.SHUTDOWN_EVENT.set()


async def _shutdown_trigger(*args, **kwargs):
    _signal_handler()


def main():
    port = os.environ.get('DLS_HTTP_PORT') or '80'

    config = Config()
    config.workers = int(os.environ.get('DLS_HTTP_WORKERS') or '4')
    config.bind = [f':::{port}']

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.new_event_loop()
    loop.add_signal_handler(signal.SIGTERM, _signal_handler)
    asyncio.set_event_loop(loop)

    # asyncio.run(serve(app, config))
    base_app.SHUTDOWN_EVENT_SUPPORTED = True
    return loop.run_until_complete(
        serve(
            app,  # type: ignore  # TODO: fix
            config,
            shutdown_trigger=_shutdown_trigger,
        ))


if __name__ == '__main__':
    main()
