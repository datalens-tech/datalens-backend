from __future__ import annotations

from typing import Optional

from control_api.app_factory import ControlApiAppFactoryNebius
import flask

from bi_api_lib_ya.app_settings import ControlPlaneAppSettings
from bi_defaults.environments import (
    EnvAliasesMap,
    InstallationsMap,
)
from bi_defaults.yenv_type import YEnvFallbackConfigResolver
from dl_api_commons.sentry_config import (
    SentryConfig,
    hook_configure_configure_sentry_for_flask,
)
from dl_api_lib.app_settings import ControlApiAppTestingsSettings
from dl_api_lib.loader import (
    ApiLibraryConfig,
    load_bi_api_lib,
    preload_bi_api_lib,
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
from dl_core.connectors.settings.registry import (
    CONNECTORS_SETTINGS_CLASSES,
    CONNECTORS_SETTINGS_FALLBACKS,
)
from dl_core.loader import CoreLibraryConfig
from dl_core.logging_config import hook_configure_logging


def create_app(
    app_settings: ControlPlaneAppSettings,
    connectors_settings: dict[ConnectionType, ConnectorSettingsBase],
    testing_app_settings: Optional[ControlApiAppTestingsSettings] = None,
    close_loop_after_request: bool = True,
) -> flask.Flask:
    mng_app_factory = ControlApiAppFactoryNebius(settings=app_settings)
    return mng_app_factory.create_app(
        connectors_settings=connectors_settings,
        testing_app_settings=testing_app_settings,
        close_loop_after_request=close_loop_after_request,
    )


def _create_gunicorn_app() -> flask.Flask:
    preload_bi_api_lib()
    fallback_resolver = YEnvFallbackConfigResolver(
        installation_map=InstallationsMap,
        env_map=EnvAliasesMap,
    )
    settings = load_settings_from_env_with_fallback(
        ControlPlaneAppSettings,
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
    app = create_app(settings, connectors_settings)

    actual_sentry_dsn: Optional[str] = settings.SENTRY_DSN if settings.SENTRY_ENABLED else None

    hook_configure_logging(
        app,
        app_name="control_api",
        app_prefix="a",
        use_jaeger_tracer=use_jaeger_tracer(),
        jaeger_service_name=jaeger_service_name_env_aware("control_api"),
    )
    if actual_sentry_dsn is not None:
        hook_configure_configure_sentry_for_flask(app, SentryConfig(dsn=actual_sentry_dsn))

    return app


gunicorn_app = _create_gunicorn_app()
