from __future__ import annotations

import logging
import os

from aiohttp import web
from app_yc_data_api_sec_embeds.app_factory import DataApiSecEmbedsAppFactoryYC

from bi_api_lib_ya.app_settings import AsyncAppSettings
from bi_defaults.environments import (
    EnvAliasesMap,
    InstallationsMap,
)
from bi_defaults.yenv_type import YEnvFallbackConfigResolver
from dl_api_lib.loader import (
    ApiLibraryConfig,
    load_bi_api_lib,
    preload_bi_api_lib,
)
from dl_app_tools.aio_latency_tracking import LatencyTracker
from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_configs.env_var_definitions import (
    jaeger_service_name_env_aware,
    use_jaeger_tracer,
)
from dl_configs.settings_loaders.loader_env import (
    load_connectors_settings_from_env_with_fallback,
    load_settings_from_env_with_fallback,
)
from dl_constants.enums import ConnectionType
from dl_core.connectors.settings.registry import (
    CONNECTORS_SETTINGS_CLASSES,
    CONNECTORS_SETTINGS_FALLBACKS,
)
from dl_core.loader import CoreLibraryConfig
from dl_core.logging_config import configure_logging


LOGGER = logging.getLogger(__name__)


def create_app(
    setting: AsyncAppSettings,
    connectors_settings: dict[ConnectionType, ConnectorSettingsBase],
) -> web.Application:
    data_api_app_factory = DataApiSecEmbedsAppFactoryYC(settings=setting)
    return data_api_app_factory.create_app(
        connectors_settings=connectors_settings,
    )


async def create_gunicorn_app(start_selfcheck: bool = True) -> web.Application:
    preload_bi_api_lib()
    fallback_resolver = YEnvFallbackConfigResolver(
        installation_map=InstallationsMap,
        env_map=EnvAliasesMap,
    )
    settings = load_settings_from_env_with_fallback(
        AsyncAppSettings,
        default_fallback_cfg_resolver=fallback_resolver,
    )
    load_bi_api_lib(
        ApiLibraryConfig(
            api_connector_ep_names=settings.BI_API_CONNECTOR_WHITELIST,
            core_lib_config=CoreLibraryConfig(core_connector_ep_names=settings.CORE_CONNECTOR_WHITELIST),
        )
    )
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
