from __future__ import annotations

from typing import Optional

from control_api import app_version
from control_api.app_factory import ControlApiAppFactoryNebius
import flask

from bi_api_lib.app_settings import ControlApiAppTestingsSettings
from bi_api_lib.loader import (
    ApiLibraryConfig,
    load_bi_api_lib,
    preload_bi_api_lib,
)
from bi_api_lib_ya.app_settings import ControlPlaneAppSettings
from bi_configs.connectors_settings import ConnectorSettingsBase
from bi_configs.env_var_definitions import (
    jaeger_service_name_env_aware,
    use_jaeger_tracer,
)
from bi_configs.settings_loaders.loader_env import (
    load_connectors_settings_from_env_with_fallback,
    load_settings_from_env_with_fallback,
)
from bi_constants.enums import ConnectionType
from bi_core.connectors.settings.registry import (
    CONNECTORS_SETTINGS_CLASSES,
    CONNECTORS_SETTINGS_FALLBACKS,
)
from bi_core.flask_utils.sentry import configure_raven_for_flask
from bi_core.logging_config import hook_configure_logging


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


def create_uwsgi_app() -> flask.Flask:
    preload_bi_api_lib()
    settings = load_settings_from_env_with_fallback(ControlPlaneAppSettings)
    load_bi_api_lib(ApiLibraryConfig(api_connector_ep_names=settings.CONNECTOR_WHITELIST))
    connectors_settings = load_connectors_settings_from_env_with_fallback(
        settings_registry=CONNECTORS_SETTINGS_CLASSES,
        fallbacks=CONNECTORS_SETTINGS_FALLBACKS,
    )
    app = create_app(settings, connectors_settings)

    actual_sentry_dsn: Optional[str] = settings.SENTRY_DSN if settings.SENTRY_ENABLED else None

    hook_configure_logging(
        app,
        app_name="control_api",
        app_prefix="a",
        sentry_dsn=actual_sentry_dsn,
        use_jaeger_tracer=use_jaeger_tracer(),
        jaeger_service_name=jaeger_service_name_env_aware("control_api"),
    )

    # Sentry in the flask app: 'BadRequest' / 'InternalServerError' / ..., 'in werkzeug/exceptions.py'.
    configure_raven_for_flask(app, actual_sentry_dsn, release=app_version)

    return app


app = create_uwsgi_app()
