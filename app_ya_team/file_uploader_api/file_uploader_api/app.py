import logging
import os

from aiohttp import web
from file_uploader_api import app_version
from file_uploader_api.app_factory import FileUploaderApiAppFactoryYT

from bi_defaults.environments import InternalProductionInstallation
from dl_configs.settings_loaders.fallback_cfg_resolver import ConstantFallbackConfigResolver
from dl_configs.settings_loaders.loader_env import load_settings_from_env_with_fallback
from dl_core.logging_config import configure_logging
from dl_file_uploader_api_lib.settings import FileUploaderAPISettings


LOGGER = logging.getLogger(__name__)


async def create_gunicorn_app() -> web.Application:
    fallback_resolver = ConstantFallbackConfigResolver(
        config=InternalProductionInstallation(),
    )
    settings = load_settings_from_env_with_fallback(
        FileUploaderAPISettings,
        default_fallback_cfg_resolver=fallback_resolver,
    )
    configure_logging(app_name="bi_file_uploader_api")
    try:
        LOGGER.info("Creating application instance...")
        app_factory = FileUploaderApiAppFactoryYT(settings=settings)
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


if __name__ == "__main__":
    main()
