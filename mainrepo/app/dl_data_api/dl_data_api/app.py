from __future__ import annotations

import logging
import os
from typing import Optional

from aiohttp import web

from bi_app_tools.aio_latency_tracking import LatencyTracker

from bi_configs.connectors_settings import ConnectorSettingsBase
from bi_configs.env_var_definitions import use_jaeger_tracer, jaeger_service_name_env_aware
from bi_configs.settings_loaders.loader_env import (
    load_settings_from_env_with_fallback, load_connectors_settings_from_env_with_fallback,
)
from bi_constants.enums import ConnectionType

from bi_core.logging_config import configure_logging

from bi_api_lib.app_settings import TestAppSettings, DataApiAppSettings
from bi_api_lib.loader import ApiLibraryConfig, preload_bi_api_lib, load_bi_api_lib

from bi_core.connectors.settings.registry import CONNECTORS_SETTINGS_CLASSES, CONNECTORS_SETTINGS_FALLBACKS

from dl_data_api.app_factory import DataApiAppFactoryOS


LOGGER = logging.getLogger(__name__)


def create_app(
        setting: DataApiAppSettings,
        connectors_settings: dict[ConnectionType, ConnectorSettingsBase],
        test_setting: Optional[TestAppSettings] = None
) -> web.Application:
    data_api_app_factory = DataApiAppFactoryOS(settings=setting)
    return data_api_app_factory.create_app(
        connectors_settings=connectors_settings,
        test_setting=test_setting,
    )


async def create_gunicorn_app(start_selfcheck: bool = True) -> web.Application:
    preload_bi_api_lib()
    settings = load_settings_from_env_with_fallback(DataApiAppSettings)
    load_bi_api_lib(ApiLibraryConfig(api_connector_ep_names=settings.CONNECTOR_WHITELIST))
    connectors_settings = load_connectors_settings_from_env_with_fallback(
        settings_registry=CONNECTORS_SETTINGS_CLASSES,
        fallbacks=CONNECTORS_SETTINGS_FALLBACKS,
    )
    configure_logging(
        app_name=settings.app_name,
        app_prefix=settings.app_prefix,
        use_jaeger_tracer=use_jaeger_tracer(),
        jaeger_service_name=jaeger_service_name_env_aware(settings.jaeger_service_name),
    )
    try:
        LOGGER.info("Creating application instance...")
        app = create_app(setting=settings, connectors_settings=connectors_settings)
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
