from __future__ import annotations

from typing import Optional

import flask

from bi_configs.connectors_settings import ConnectorSettingsBase
from bi_configs.env_var_definitions import use_jaeger_tracer, jaeger_service_name_env_aware
from bi_configs.settings_loaders.loader_env import (
    load_settings_from_env_with_fallback, load_connectors_settings_from_env_with_fallback,
)
from bi_constants.enums import ConnectionType

from bi_core.connectors.settings.registry import CONNECTORS_SETTINGS_CLASSES, CONNECTORS_SETTINGS_FALLBACKS
from bi_core.flask_utils.sentry import configure_raven_for_flask
from bi_core.logging_config import hook_configure_logging

from bi_api_lib.app_settings import ControlPlaneAppSettings, ControlPlaneAppTestingsSettings
from bi_api_lib.loader import ApiLibraryConfig, load_bi_api_lib

from app_yc_control_api.app_factory import ControlApiAppFactoryYC
from app_yc_control_api import app_version


def create_app(
        app_settings: ControlPlaneAppSettings,
        connectors_settings: dict[ConnectionType, ConnectorSettingsBase],
        testing_app_settings: Optional[ControlPlaneAppTestingsSettings] = None,
        close_loop_after_request: bool = True,
) -> flask.Flask:
    mng_app_factory = ControlApiAppFactoryYC()
    return mng_app_factory.create_app(
        app_settings=app_settings,
        connectors_settings=connectors_settings,
        testing_app_settings=testing_app_settings,
        close_loop_after_request=close_loop_after_request,
    )


def create_uwsgi_app() -> flask.Flask:
    settings = load_settings_from_env_with_fallback(ControlPlaneAppSettings)
    load_bi_api_lib(ApiLibraryConfig(api_connector_ep_names=settings.CONNECTOR_WHITELIST))
    connectors_settings = load_connectors_settings_from_env_with_fallback(
        settings_registry=CONNECTORS_SETTINGS_CLASSES,
        fallbacks=CONNECTORS_SETTINGS_FALLBACKS,
    )
    uwsgi_app = create_app(settings, connectors_settings)

    actual_sentry_dsn: Optional[str] = settings.SENTRY_DSN if settings.SENTRY_ENABLED else None

    hook_configure_logging(
        uwsgi_app,
        app_name='bi_api_app',
        app_prefix='a',
        sentry_dsn=actual_sentry_dsn,
        use_jaeger_tracer=use_jaeger_tracer(),
        jaeger_service_name=jaeger_service_name_env_aware('bi-api-app'),
    )

    # Sentry in the flask app: 'BadRequest' / 'InternalServerError' / ..., 'in werkzeug/exceptions.py'.
    configure_raven_for_flask(uwsgi_app, actual_sentry_dsn, release=app_version)

    return uwsgi_app


app = create_uwsgi_app()
