from __future__ import annotations

import logging
import os
from typing import Optional

from aiohttp import web

from bi_app_tools.aio_latency_tracking import LatencyTracker

from bi_configs.env_var_definitions import use_jaeger_tracer, jaeger_service_name_env_aware
from bi_configs.settings_loaders.loader_env import load_settings_from_env_with_fallback

from bi_core.logging_config import configure_logging

from bi_api_lib.app_settings import AsyncAppSettings, TestAppSettings

from bi_meta_yc_data_api_sec_embeds.app import DataApiSecEmbedsAppFactoryYC
from app_yc_data_api_sec_embeds import app_version


LOGGER = logging.getLogger(__name__)


class DefaultDataApiSecEmbedsAppFactoryYC(DataApiSecEmbedsAppFactoryYC):
    def get_app_version(self) -> str:
        return app_version


# TODO CONSIDER: Pass all testing workarounds in constructor args
def create_app(setting: AsyncAppSettings, test_setting: Optional[TestAppSettings] = None) -> web.Application:
    data_api_app_factory = DefaultDataApiSecEmbedsAppFactoryYC()
    return data_api_app_factory.create_app(
        setting=setting,
        test_setting=test_setting,
    )


async def create_gunicorn_app(start_selfcheck: bool = True) -> web.Application:
    settings = load_settings_from_env_with_fallback(AsyncAppSettings)
    configure_logging(
        app_name=settings.app_name,
        # TODO FIX: Find place to store logic of prefix selection
        app_prefix=settings.app_prefix,
        use_jaeger_tracer=use_jaeger_tracer(),
        jaeger_service_name=jaeger_service_name_env_aware(settings.jaeger_service_name),
    )
    try:
        LOGGER.info("Creating application instance...")
        app = create_app(setting=settings)
        if start_selfcheck:
            LOGGER.info("Starting selfcheck aio task...")
            await LatencyTracker().run_task()
        LOGGER.info("Application instance was created")
        return app
    except Exception:
        LOGGER.exception("Exception during app creation")
        raise


def main() -> None:
    host = os.environ["APP_HOST"]
    port = int(os.environ["APP_PORT"])
    app_coro = create_gunicorn_app()
    web.run_app(app_coro, host=host, port=port)


if __name__ == "__main__":
    main()
