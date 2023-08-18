import logging
import os

from aiohttp import web

from bi_configs.settings_loaders.loader_env import load_settings_from_env_with_fallback
from bi_core.logging_config import configure_logging

from bi_file_uploader_api_lib.app import FileUploaderApiAppFactory
from bi_file_uploader_api_lib.settings import FileUploaderAPISettings

from bi_file_uploader import app_version


LOGGER = logging.getLogger(__name__)


class DefaultFileUploaderApiAppFactory(FileUploaderApiAppFactory):
    pass


async def create_gunicorn_app() -> web.Application:
    settings = load_settings_from_env_with_fallback(FileUploaderAPISettings)
    configure_logging(app_name='bi_file_uploader_api')
    try:
        LOGGER.info("Creating application instance...")
        app_factory = DefaultFileUploaderApiAppFactory(settings=settings)
        app = app_factory.create_app(app_version)
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


if __name__ == '__main__':
    main()
