import asyncio
import logging

from dl_api_commons.sentry_config import (
    SentryConfig,
    configure_sentry,
)
from dl_configs.settings_loaders.loader_env import load_settings_from_env_with_fallback
from dl_configs.utils import get_root_certificates
from dl_core.logging_config import configure_logging
from dl_file_uploader_worker import app_version
from dl_file_uploader_worker.app_factory import StandaloneFileUploaderWorkerFactory
from dl_file_uploader_worker.app_settings import FileUploaderWorkerSettingsOS
from dl_task_processor.worker import HealthChecker


LOGGER = logging.getLogger(__name__)


def load_settings() -> FileUploaderWorkerSettingsOS:
    settings = load_settings_from_env_with_fallback(FileUploaderWorkerSettingsOS)
    return settings


def run_standalone_worker() -> None:
    loop = asyncio.get_event_loop()
    settings = load_settings()
    ca_data = get_root_certificates(path=settings.CA_FILE_PATH)
    worker = StandaloneFileUploaderWorkerFactory(settings=settings, ca_data=ca_data).create_worker()
    configure_logging(
        app_name="dl_file_uploader_worker",
    )
    if settings.SENTRY_DSN is not None:
        configure_sentry(SentryConfig(dsn=settings.SENTRY_DSN, release=app_version))
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
    settings = load_settings()
    configure_logging(
        app_name="dl_file_uploader_worker_health_check",
    )
    ca_data = get_root_certificates(path=settings.CA_FILE_PATH)
    if settings.SENTRY_DSN is not None:
        configure_sentry(SentryConfig(dsn=settings.SENTRY_DSN, release=app_version))
    worker = StandaloneFileUploaderWorkerFactory(settings=settings, ca_data=ca_data).create_worker()
    health_checker = HealthChecker(worker)
    loop.run_until_complete(health_checker.check())
