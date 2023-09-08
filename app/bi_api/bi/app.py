from __future__ import annotations

import os
from typing import Optional

import flask

from bi_configs.connectors_settings import ConnectorSettingsBase
from bi_configs.env_var_definitions import use_jaeger_tracer, jaeger_service_name_env_aware
from bi_configs.settings_loaders.fallback_cfg_resolver import YEnvFallbackConfigResolver
from bi_configs.settings_loaders.loader_env import (
    load_settings_from_env_with_fallback_legacy, load_connectors_settings_from_env_with_fallback,
)
from bi_constants.enums import ConnectionType

from bi import app_version
from bi_core.connectors.settings.registry import CONNECTORS_SETTINGS_CLASSES, CONNECTORS_SETTINGS_FALLBACKS

from bi_core.flask_utils.sentry import configure_raven_for_flask
from bi_core.logging_config import hook_configure_logging

from bi_api_lib_ya.app_common import LegacySRFactoryBuilder
from bi_api_lib_ya.app.control_api.app import LegacyControlApiAppFactory
from bi_api_lib.app_settings import ControlApiAppTestingsSettings
from bi_api_lib_ya.app_settings import ControlPlaneAppSettings
from bi_api_lib.loader import ApiLibraryConfig, preload_bi_api_lib, load_bi_api_lib

from bi_configs.environments import InstallationsMap, EnvAliasesMap


class DefaultControlApiAppFactory(LegacyControlApiAppFactory, LegacySRFactoryBuilder):
    """The "real" app factory that is used in runtime"""


def create_app(
        app_settings: ControlPlaneAppSettings,
        connectors_settings: dict[ConnectionType, ConnectorSettingsBase],
        testing_app_settings: Optional[ControlApiAppTestingsSettings] = None,
        close_loop_after_request: bool = True,
) -> flask.Flask:
    mng_app_factory = DefaultControlApiAppFactory(settings=app_settings)
    return mng_app_factory.create_app(
        connectors_settings=connectors_settings,
        testing_app_settings=testing_app_settings,
        close_loop_after_request=close_loop_after_request,
    )


def create_uwsgi_app():  # type: ignore  # TODO: fix
    preload_bi_api_lib()
    fallback_resolver = YEnvFallbackConfigResolver(
        installation_map=InstallationsMap,
        env_map=EnvAliasesMap,
    )
    settings = load_settings_from_env_with_fallback_legacy(
        ControlPlaneAppSettings,
        fallback_cfg_resolver=fallback_resolver,
    )
    load_bi_api_lib(ApiLibraryConfig(api_connector_ep_names=settings.BI_API_CONNECTOR_WHITELIST))
    connectors_settings = load_connectors_settings_from_env_with_fallback(
        settings_registry=CONNECTORS_SETTINGS_CLASSES,
        fallbacks=CONNECTORS_SETTINGS_FALLBACKS,
        fallback_cfg_resolver=fallback_resolver,
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


app = None

# TODO FIX: BI-2497 move launch target extraction to bi_config
if os.environ.get("LAUNCH_TARGET") == "sync_api":
    app = create_uwsgi_app()
