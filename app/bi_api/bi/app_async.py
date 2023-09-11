from __future__ import annotations

import logging
import os

from aiohttp import web

from bi_app_tools.aio_latency_tracking import LatencyTracker

from bi_configs.connectors_settings import ConnectorSettingsBase
from bi_configs.env_var_definitions import use_jaeger_tracer, jaeger_service_name_env_aware
from bi_configs.settings_loaders.fallback_cfg_resolver import YEnvFallbackConfigResolver
from bi_configs.settings_loaders.loader_env import (
    load_settings_from_env_with_fallback_legacy, load_connectors_settings_from_env_with_fallback,
)
from bi_constants.enums import ConnectionType

from bi_core.logging_config import configure_logging

from bi_api_lib.loader import ApiLibraryConfig, preload_bi_api_lib, load_bi_api_lib
from bi_api_lib_ya.app_common import LegacySRFactoryBuilder
from bi_api_lib_ya.app_settings import AsyncAppSettings
from bi_api_lib_ya.app.data_api.app import LegacyDataApiAppFactory

from bi_core.connectors.settings.registry import CONNECTORS_SETTINGS_CLASSES, CONNECTORS_SETTINGS_FALLBACKS

from bi import app_version
from bi_configs.environments import InstallationsMap, EnvAliasesMap

LOGGER = logging.getLogger(__name__)


class DefaultDataApiAppFactory(LegacyDataApiAppFactory, LegacySRFactoryBuilder):
    def get_app_version(self) -> str:
        return app_version


def create_app(
        setting: AsyncAppSettings,
        connectors_settings: dict[ConnectionType, ConnectorSettingsBase],
) -> web.Application:
    data_api_app_factory = DefaultDataApiAppFactory(settings=setting)
    return data_api_app_factory.create_app(
        connectors_settings=connectors_settings,
    )


async def create_gunicorn_app(start_selfcheck: bool = True) -> web.Application:
    preload_bi_api_lib()
    fallback_resolver = YEnvFallbackConfigResolver(
        installation_map=InstallationsMap,
        env_map=EnvAliasesMap,
    )
    settings = load_settings_from_env_with_fallback_legacy(
        AsyncAppSettings,
        fallback_cfg_resolver=fallback_resolver,
    )
    load_bi_api_lib(ApiLibraryConfig(api_connector_ep_names=settings.BI_API_CONNECTOR_WHITELIST))
    connectors_settings = load_connectors_settings_from_env_with_fallback(
        settings_registry=CONNECTORS_SETTINGS_CLASSES,
        fallbacks=CONNECTORS_SETTINGS_FALLBACKS,
        fallback_cfg_resolver=fallback_resolver,
    )
    configure_logging(
        app_name=settings.app_name,
        # TODO FIX: Find place to store logic of prefix selection
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
