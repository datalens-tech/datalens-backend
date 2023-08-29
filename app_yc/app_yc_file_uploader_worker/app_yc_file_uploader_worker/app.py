from typing import List

import logging
import asyncio
from aiohttp import web

from bi_core.logging_config import configure_logging
from bi_configs.settings_loaders.loader_env import load_settings_from_env_with_fallback
from bi_task_processor.arq_wrapper import create_redis_pool, create_arq_redis_settings
from bi_task_processor.controller import Cli
from bi_task_processor.processor import ARQProcessorImpl, TaskProcessor
from bi_task_processor.state import TaskState, DummyStateImpl
from bi_task_processor.worker import HealthChecker

from bi_file_uploader_worker_lib.tasks import REGISTRY
from bi_file_uploader_worker_lib.settings import FileUploaderWorkerSettings

from app_yc_file_uploader_worker.app_factory import FileUploaderWorkerFactoryYC


LOGGER = logging.getLogger(__name__)


def run_standalone_worker() -> None:
    loop = asyncio.get_event_loop()
    settings = load_settings_from_env_with_fallback(FileUploaderWorkerSettings)
    worker = FileUploaderWorkerFactoryYC(settings=settings).create_worker()
    configure_logging(app_name='bi_file_uploader_worker', sentry_dsn=settings.SENTRY_DSN)
    worker_task = loop.create_task(worker.start())
    try:
        loop.run_forever()
    except asyncio.CancelledError:  # pragma: no cover
        # happens on shutdown, fine
        pass
    finally:
        worker_task.cancel()
        loop.run_until_complete(worker.stop())
        loop.close()


def run_health_check() -> None:
    loop = asyncio.get_event_loop()
    settings = load_settings_from_env_with_fallback(FileUploaderWorkerSettings)
    configure_logging(app_name='bi_file_uploader_worker_health_check', sentry_dsn=settings.SENTRY_DSN)
    worker = FileUploaderWorkerFactoryYC(settings=settings).create_worker()
    health_checker = HealthChecker(worker)
    loop.run_until_complete(health_checker.check())


def run_cli(args: List) -> None:
    parsed_args = Cli.parse_params(args)
    loop = asyncio.get_event_loop()
    settings = load_settings_from_env_with_fallback(FileUploaderWorkerSettings)
    configure_logging(app_name='bi_file_uploader_cli')
    redis_pool = loop.run_until_complete(
        create_redis_pool(
            create_arq_redis_settings(settings.REDIS_ARQ),
        ),
    )
    impl = ARQProcessorImpl(redis_pool)
    state = TaskState(DummyStateImpl())
    processor = TaskProcessor(impl=impl, state=state)
    cli = Cli(processor=processor, registry=REGISTRY)
    loop.run_until_complete(cli.run(parsed_args))


async def create_secure_reader_gunicorn_app() -> web.Application:
    from bi_file_secure_reader.app import create_app as create_secure_reader_app

    try:
        LOGGER.info("Creating application instance")
        app = create_secure_reader_app()
        LOGGER.info("Application instance was created")
        return app
    except Exception:
        LOGGER.exception("Exception during app creation")
        raise


if __name__ == '__main__':
    run_standalone_worker()
