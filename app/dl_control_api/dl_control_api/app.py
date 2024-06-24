from __future__ import annotations

from typing import Optional

import flask

from dl_api_commons.sentry_config import (
    SentryConfig,
    hook_configure_configure_sentry_for_flask,
)
from dl_api_lib.app_settings import (
    ControlApiAppSettingsOS,
    ControlApiAppTestingsSettings,
)
from dl_api_lib.loader import (
    ApiLibraryConfig,
    load_api_lib,
    preload_api_lib,
)
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
from dl_control_api import app_version
from dl_control_api.app_factory import StandaloneControlApiAppFactory
from dl_core.connectors.settings.registry import (
    CONNECTORS_SETTINGS_CLASSES,
    CONNECTORS_SETTINGS_FALLBACKS,
)
from dl_core.loader import CoreLibraryConfig
from dl_core.logging_config import hook_configure_logging


def create_app(
    app_settings: ControlApiAppSettingsOS,
    connectors_settings: dict[ConnectionType, ConnectorSettingsBase],
    testing_app_settings: Optional[ControlApiAppTestingsSettings] = None,
    close_loop_after_request: bool = True,
) -> flask.Flask:
    mng_app_factory = StandaloneControlApiAppFactory(settings=app_settings)
    return mng_app_factory.create_app(
        connectors_settings=connectors_settings,
        testing_app_settings=testing_app_settings,
        close_loop_after_request=close_loop_after_request,
    )


def create_uwsgi_app() -> flask.Flask:
    preload_api_lib()
    settings = load_settings_from_env_with_fallback(ControlApiAppSettingsOS)
    load_api_lib(
        ApiLibraryConfig(
            api_connector_ep_names=settings.BI_API_CONNECTOR_WHITELIST,
            core_lib_config=CoreLibraryConfig(core_connector_ep_names=settings.CORE_CONNECTOR_WHITELIST),
        )
    )
    connectors_settings = load_connectors_settings_from_env_with_fallback(
        settings_registry=CONNECTORS_SETTINGS_CLASSES,
        fallbacks=CONNECTORS_SETTINGS_FALLBACKS,
    )
    uwsgi_app = create_app(settings, connectors_settings)

    actual_sentry_dsn: Optional[str] = settings.SENTRY_DSN if settings.SENTRY_ENABLED else None

    hook_configure_logging(
        uwsgi_app,
        app_name="dl_api_app",
        app_prefix="a",
        use_jaeger_tracer=use_jaeger_tracer(),
        jaeger_service_name=jaeger_service_name_env_aware("dl-api-app"),
    )
    if actual_sentry_dsn is not None:
        hook_configure_configure_sentry_for_flask(
            uwsgi_app,
            SentryConfig(dsn=actual_sentry_dsn, release=app_version),
        )

    return uwsgi_app


app = create_uwsgi_app()
