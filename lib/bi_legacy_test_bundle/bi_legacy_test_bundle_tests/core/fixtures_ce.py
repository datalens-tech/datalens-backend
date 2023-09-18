from __future__ import annotations

import socket
import time
import multiprocessing
import logging

import opentracing
import pytest
import requests

from dl_configs.env_var_definitions import use_jaeger_tracer
from dl_configs.rqe import RQEBaseURL

from dl_core.connection_executors.models.common import RemoteQueryExecutorData
from dl_core.connection_executors.remote_query_executor.app_async import create_async_qe_app
from dl_core.connection_executors.remote_query_executor.app_sync import create_sync_app


@pytest.fixture(scope='function')
def query_executor_app(loop, aiohttp_client, bi_config):
    app = create_async_qe_app(
        hmac_key=bi_config.EXT_QUERY_EXECUTER_SECRET_KEY.encode(),
    )
    return loop.run_until_complete(aiohttp_client(app))


SYNC_RQE_HOST = '127.0.0.1'
SYNC_RQE_BIND = SYNC_RQE_HOST
SYNC_RQE_WAIT_TIME = 15.0
SYNC_RQE_POLL_TIME = 0.1


def get_free_port(bind):
    # Binding on random port
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind((bind, 0))
        port = sock.getsockname()[1]
    finally:
        sock.close()
    # And hope that nobody binds on it before Flask.
    return port


def run_flask(host: str, port: int, hmac_key: bytes, init_jaeger_tracer: bool):
    import signal
    import sys
    import jaeger_client

    from dl_core.logging_config import setup_jaeger_client

    app = create_sync_app(
        hmac_key=hmac_key,
    )
    # Stripped down `app.run(host=host, port=port, load_dotenv=False)`:
    from werkzeug.serving import run_simple

    if init_jaeger_tracer:
        def on_exit(_, __):
            tracer_to_terminate = opentracing.global_tracer()
            if isinstance(tracer_to_terminate, jaeger_client.Tracer):
                tracer_to_terminate.close()
                # TODO FIX: BI-1967 awaiting of tracer.close() hangs forever
                time.sleep(0.05)

            sys.exit(0)

        signal.signal(signal.SIGTERM, on_exit)

        setup_jaeger_client('tests_sync_rqe_fixture')

    return run_simple(
        host, port, app,
        use_reloader=False, use_debugger=False, threaded=True)


@pytest.fixture(scope='session')
def sync_remote_query_executor(bi_config) -> RQEBaseURL:
    # TODO FIX: Use bi_core_testing.fixture_server_runner.WSGIRunner when logging issues will be resolved
    logger = logging.getLogger('rqe_mgr')
    sync_rqe_bind = SYNC_RQE_BIND
    sync_rqe_host = SYNC_RQE_HOST

    init_jaeger_tracer = use_jaeger_tracer()

    ctx = multiprocessing.get_context('spawn') if init_jaeger_tracer else multiprocessing.get_context('fork')

    logger.debug('Finding an available port...')
    sync_rqe_port = get_free_port(bind=sync_rqe_bind)

    logger.debug('Starting QE at %r:%r', sync_rqe_bind, sync_rqe_port)
    qe_proc = ctx.Process(
        target=run_flask,
        args=(sync_rqe_bind, sync_rqe_port, bi_config.EXT_QUERY_EXECUTER_SECRET_KEY.encode(), init_jaeger_tracer)
    )
    qe_proc.start()

    start_time = time.monotonic()
    max_time = start_time + SYNC_RQE_WAIT_TIME
    last_error = None
    resp = None
    url = f'http://{sync_rqe_host}:{sync_rqe_port}/ping'
    while True:
        next_attempt_time = time.monotonic() + SYNC_RQE_POLL_TIME
        try:
            resp = requests.get(url, timeout=SYNC_RQE_POLL_TIME)
        except Exception as err:
            last_error = err
        else:
            if resp.ok:
                break
        logger.debug("Still waiting for QE: err=%r / resp=%r", last_error, resp)
        now = time.monotonic()
        if now > max_time:
            raise Exception(
                f"Timed out waiting for SYNC_RQE to come up at {url}",
                dict(
                    last_error=last_error,
                    last_resp=resp,
                    last_resp_text=resp.text if resp is not None else None))
        sleep_time = next_attempt_time - now
        if sleep_time > 0.001:
            time.sleep(sleep_time)

    logger.debug("QE up.")
    yield RQEBaseURL(host=sync_rqe_host, port=sync_rqe_port)

    logger.debug("Getting QE down...")
    qe_proc.terminate()
    with opentracing.global_tracer().start_active_span("await_proc_to_finish"):
        try:
            qe_proc.join(3)
        except Exception:
            logger.warning("QE did not finish in 3 seconds, doing a SIGKILL.")
            qe_proc.kill()


@pytest.fixture(scope='function')
def query_executor_options(query_executor_app, sync_remote_query_executor, bi_config) -> RemoteQueryExecutorData:
    return RemoteQueryExecutorData(
        hmac_key=bi_config.EXT_QUERY_EXECUTER_SECRET_KEY.encode(),
        # Async RQE
        async_protocol='http',
        async_host=query_executor_app.host,
        async_port=query_executor_app.port,
        # Sync RQE
        sync_protocol='http',
        sync_host=sync_remote_query_executor.host,
        sync_port=sync_remote_query_executor.port,
    )
