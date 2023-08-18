from __future__ import annotations

from typing import Optional

import flask

from bi_configs.env_var_definitions import use_jaeger_tracer, jaeger_service_name_env_aware
from bi_configs.settings_loaders.loader_env import load_settings_from_env_with_fallback

from bi_core.flask_utils.sentry import configure_raven_for_flask
from bi_core.logging_config import hook_configure_logging

from bi_api_lib.app_settings import ControlPlaneAppSettings, ControlPlaneAppTestingsSettings

from bi_meta_yc_control_api.app import ControlApiAppFactoryYC
from app_yc_control_api import app_version


class DefaultControlApiAppFactoryYC(ControlApiAppFactoryYC):
    pass


def create_app(
        app_settings: ControlPlaneAppSettings,
        testing_app_settings: Optional[ControlPlaneAppTestingsSettings] = None,
        close_loop_after_request: bool = True,
) -> flask.Flask:
    mng_app_factory = DefaultControlApiAppFactoryYC()
    return mng_app_factory.create_app(
        app_settings=app_settings,
        testing_app_settings=testing_app_settings,
        close_loop_after_request=close_loop_after_request,
    )


def create_uwsgi_app() -> flask.Flask:
    settings = load_settings_from_env_with_fallback(ControlPlaneAppSettings)
    uwsgi_app = create_app(settings)

    actual_sentry_dsn: Optional[str] = settings.SENTRY_DSN_API if settings.SENTRY_ENABLED else None

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
