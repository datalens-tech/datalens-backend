import logging
import socket
from typing import Optional

from aiohttp import web

from bi_api_commons.aiohttp.aiohttp_wrappers import DLRequestBase
from bi_api_commons.aio.middlewares.request_bootstrap import RequestBootstrap
from bi_api_commons.aio.middlewares.request_id import RequestId
from bi_core.logging_config import configure_logging

from bi_testing_db_provision.aiohttp_tools.brigadier_service import BrigadierService
from bi_testing_db_provision.settings import AppSettings, DEVELOPMENT_SETTINGS
from bi_testing_db_provision.workers.worker_resource_docker import ResourceProvisioningWorker, ResourceRecyclingWorker


LOGGER = logging.getLogger(__name__)


def create_app(settings: Optional[AppSettings] = None) -> web.Application:
    LOGGER.info("Creating gunicorn app")

    effective_settings: AppSettings
    if settings is None:
        effective_settings = DEVELOPMENT_SETTINGS
    else:
        effective_settings = settings

    async def ping(_: web.Request) -> web.Response:
        return web.json_response({})

    req_id_service = RequestId(
        dl_request_cls=DLRequestBase,
    )

    app = web.Application(
        middlewares=[
            RequestBootstrap(
                req_id_service=req_id_service,
            ).middleware,
        ]
    )
    app.on_response_prepare.append(req_id_service.on_response_prepare)
    app.router.add_get("/ping", ping)

    if effective_settings.launch_api:
        # TODO FIX: Add endpoints here
        pass

    if effective_settings.launch_workers:
        workers_config = {
            ResourceRecyclingWorker: effective_settings.workers_count_recycling,
            ResourceProvisioningWorker: effective_settings.workers_count_provisioning,
        }

        effective_worker_id_prefix = (
            socket.gethostname() if settings.workers_id_prefix is None  # type: ignore  # TODO: fix
            else settings.workers_id_prefix  # type: ignore  # TODO: fix
        )

        brigadier_service = BrigadierService(
            pg_config=effective_settings.pg_config,
            worker_id_prefix=effective_worker_id_prefix,
            workers_config=workers_config,  # type: ignore  # TODO: fix
        )

        app.on_startup.append(brigadier_service.on_startup)
        app.on_cleanup.append(brigadier_service.on_shutdown)

    return app


async def create_gunicorn_app() -> web.Application:
    configure_logging(
        app_name='sklad_fixtur',
        app_prefix='skfx',
    )
    try:
        LOGGER.info("Creating application instance")
        app = create_app()
        LOGGER.info("Application instance was created")
        return app
    except Exception:
        LOGGER.exception("Exception during app creation")
        raise
