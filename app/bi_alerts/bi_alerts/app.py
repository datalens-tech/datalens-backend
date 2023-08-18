from __future__ import annotations

import os
import logging

from aiohttp import web
from aiopg.sa import create_engine

from bi_configs.crypto_keys import get_crypto_keys_config_from_env

from bi_api_commons.aio.middlewares.commit_rci import commit_rci_middleware
from bi_api_commons.aio.middlewares.request_bootstrap import RequestBootstrap
from bi_api_commons.aio.middlewares.request_id import RequestId
from bi_core.logging_config import configure_logging
from bi_core.us_manager.crypto.main import CryptoController

from bi_alerts import app_version
from bi_alerts.resources import data, ping, alert
from bi_alerts.settings import from_granular_settings, APP_KEY_SETTINGS
from bi_alerts.middlewares.error_middleware import AlertsErrorHandler
from bi_alerts.utils.solomon import SolomonClient


LOGGER = logging.getLogger(__name__)


async def setup_db(app):  # type: ignore  # TODO: fix
    app.db = await create_engine(**app[APP_KEY_SETTINGS].SQLA_DB_CFG_MASTER)


async def close_db(app):  # type: ignore  # TODO: fix
    app.db.close()
    await app.db.wait_closed()


async def close_solomon(app):  # type: ignore  # TODO: fix
    await app.solomon._session.close()


def create_app(  # type: ignore  # TODO: fix
    settings=None,
    crypto_config=None,
    enable_sentry=False,
):
    if settings is None:
        settings = from_granular_settings()

    req_id_service = RequestId(
        app_prefix="alerts",
    )

    error_handler = AlertsErrorHandler(
        sentry_app_name_tag=None,
    )

    app = web.Application(middlewares=[
        RequestBootstrap(
            req_id_service=req_id_service,
            error_handler=error_handler,
        ).middleware,
        commit_rci_middleware(),
    ])

    app[APP_KEY_SETTINGS] = settings
    app.on_startup.append(setup_db)
    app.on_cleanup.append(close_db)

    app.router.add_route('*', '/ping', ping.PingView)
    app.router.add_route('*', '/alerts/test_data', data.ChartsTestData)
    app.router.add_route('*', '/alerts/data', data.ChartsData)
    app.router.add_route('*', '/alerts/ping', ping.PingView)
    app.router.add_route('*', '/alerts/ping_db', ping.PingDbView)
    app.router.add_route('*', '/alerts/v1/alert', alert.AlertView)
    app.router.add_route('*', '/alerts/v1/alert/{alert_id}', alert.AlertInfoView)
    app.router.add_route('*', '/alerts/v1/list', alert.AlertsListView)
    app.router.add_route('*', '/alerts/v1/check', alert.AlertsCheckView)

    app.solomon = SolomonClient(
        url=settings.SOLOMON_BASE_URL,
        token=settings.SOLOMON_TOKEN,
        project=settings.SOLOMON_PROJECT,
        prefix=settings.SOLOMON_PREFIX,
        tvm_id=settings.SOLOMON_TVM_ID,
    )
    app.on_cleanup.append(close_solomon)

    crypto_keys_config = get_crypto_keys_config_from_env(crypto_config)
    app.crypto = CryptoController(crypto_keys_config)

    if enable_sentry and settings.SENTRY_DSN is not None:
        import sentry_sdk
        from sentry_sdk.integrations.aiohttp import AioHttpIntegration
        from sentry_sdk.integrations.logging import LoggingIntegration

        logging_integration = LoggingIntegration(
            level=logging.INFO,
            event_level=logging.WARNING,
        )

        sentry_sdk.init(
            dsn=settings.SENTRY_DSN,
            integrations=[
                AioHttpIntegration(),
                logging_integration,
            ],
            release=app_version,
        )

    return app


async def create_gunicorn_app():  # type: ignore  # TODO: fix
    configure_logging(
        app_name='bi_alerts',
    )

    try:
        LOGGER.info("Creating application instance")
        app = create_app(enable_sentry=True)
        LOGGER.info("Application instance was created")
        return app
    except Exception:
        LOGGER.exception("Exception during app creation")
        raise


def main():  # type: ignore  # TODO: fix
    current_app = create_gunicorn_app()
    web.run_app(
        current_app,
        host=os.environ["APP_HOST"],
        port=int(os.environ["APP_PORT"]),
    )


if __name__ == '__main__':
    main()
