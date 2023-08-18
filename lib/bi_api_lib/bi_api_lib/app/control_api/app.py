from __future__ import annotations

import abc
from typing import Optional, TYPE_CHECKING

import attr
import flask
from flask import Flask
from flask_marshmallow import Marshmallow

from bi_api_commons.flask.middlewares.commit_rci_middleware import ReqCtxInfoMiddleware
from bi_api_commons.flask.middlewares.context_var_middleware import ContextVarMiddleware
from bi_api_commons.flask.middlewares.logging_context import RequestLoggingContextControllerMiddleWare
from bi_api_commons.flask.middlewares.request_id import RequestIDService
from bi_api_commons.yc_access_control_model import AuthorizationModeYandexCloud, AuthorizationModeDataCloud
from bi_api_commons_ya_cloud.flask.middlewares.yc_auth import FlaskYCAuthService
from bi_api_commons_ya_cloud.yc_auth import make_default_yc_auth_service_config

from bi_constants.enums import USAuthMode
from bi_configs.enums import AppType

from bi_api_lib.loader import load_bi_api_lib
from bi_api_lib.app_settings import (
    ControlPlaneAppSettings,
    ControlPlaneAppTestingsSettings,
)

from bi_core import profiling_middleware
from bi_core.flask_utils.aio_event_loop_middleware import AIOEventLoopMiddleware
from bi_core.flask_utils.services_registry_middleware import ServicesRegistryMiddleware
from bi_core.flask_utils.tracing import TracingMiddleware, TracingContextMiddleware
from bi_core.flask_utils.us_manager_middleware import USManagerFlaskMiddleware
from bi_core.ping import register_ping_handler_hax

from statcommons.unistat.flask import register_unistat_hax, register_metrics

from bi_api_lib.app_common import SRFactoryBuilder
from bi_api_lib.app_common_settings import ConnOptionsMutatorsFactory
from bi_api_lib.app.control_api.resources import init_apis

if TYPE_CHECKING:
    from bi_core.connection_models import ConnectOptions
    from bi_core.us_connection_base import ExecutorBasedMixin


@attr.s(frozen=True)
class EnvSetupResult:
    us_auth_mode: USAuthMode = attr.ib(kw_only=True)


class ControlApiAppFactory(SRFactoryBuilder, abc.ABC):
    IS_ASYNC_ENV = False

    def push_settings_to_flask_app_config(
            self,
            target_app: Flask,
            app_settings: ControlPlaneAppSettings,
    ) -> None:
        cfg = target_app.config

        cfg["US_MASTER_TOKEN"] = app_settings.US_MASTER_TOKEN
        cfg["US_HOST"] = app_settings.US_BASE_URL

        cfg["SAMPLES_CH_HOST"] = app_settings.SAMPLES_CH_HOSTS

        cfg["BLACKBOX_RETRY_PARAMS"] = app_settings.BLACKBOX_RETRY_PARAMS
        cfg["BLACKBOX_TIMEOUT"] = app_settings.BLACKBOX_TIMEOUT
        cfg["ENABLE_COOKIES_AUTH"] = False

        # Auth
        if app_settings.YC_AUTH_SETTINGS:
            cfg["YC_AS_ENDPOINT"] = app_settings.YC_AUTH_SETTINGS.YC_AS_ENDPOINT
            if app_settings.APP_TYPE == AppType.CLOUD:
                cfg["YC_AUTHORIZE_PERMISSION"] = app_settings.YC_AUTH_SETTINGS.YC_AUTHORIZE_PERMISSION
        cfg["YC_TS_ENDPOINT"] = app_settings.YC_IAM_TS_ENDPOINT
        # cfg["BLACKBOX_NAME"] = app_settings.BLACKBOX_NAME

        # DLS
        cfg["DLS_HOST"] = app_settings.DLS_HOST
        cfg["DLS_API_KEY"] = app_settings.DLS_API_KEY

        # Other YC endpoints
        cfg["CLOUD_API_IAM_ENDPOINT"] = app_settings.YC_IAM_CP_ENDPOINT
        cfg["CLOUD_API_RM_ENDPOINT"] = app_settings.YC_RM_CP_ENDPOINT
        cfg["YC_BILLING_HOST"] = app_settings.YC_BILLING_HOST

    def set_up_environment(
            self,
            app: flask.Flask, app_settings: ControlPlaneAppSettings,
            testing_app_settings: Optional[ControlPlaneAppTestingsSettings] = None,
    ) -> EnvSetupResult:
        # TODO: Split into env-specific implementation classes

        us_auth_mode: USAuthMode
        if app_settings.APP_TYPE == AppType.INTRANET:
            from bi_api_commons_ya_team.flask.middlewares import blackbox_auth
            blackbox_auth.set_up(app)
            us_auth_mode = USAuthMode.regular
        elif app_settings.APP_TYPE in (AppType.CLOUD, AppType.NEBIUS):
            yc_auth_settings = app_settings.YC_AUTH_SETTINGS
            assert yc_auth_settings is not None, "app_settings.YC_AUTH_SETTINGS must not be None with AppType.CLOUD"

            FlaskYCAuthService(
                authorization_mode=AuthorizationModeYandexCloud(
                    folder_permission_to_check=yc_auth_settings.YC_AUTHORIZE_PERMISSION,
                    organization_permission_to_check=yc_auth_settings.YC_AUTHORIZE_PERMISSION,
                ),
                enable_cookie_auth=False,
                access_service_cfg=make_default_yc_auth_service_config(yc_auth_settings.YC_AS_ENDPOINT),
            ).set_up(app)
            us_auth_mode = USAuthMode.regular
        elif app_settings.APP_TYPE == AppType.TESTS:
            from bi_core.flask_utils.trust_auth import TrustAuthService
            TrustAuthService(
                fake_user_id='_the_tests_syncapp_user_id_',
                fake_user_name='_the_tests_syncapp_user_name_',
                fake_tenant=None if testing_app_settings is None else testing_app_settings.fake_tenant
            ).set_up(app)

            us_auth_mode_override = None if testing_app_settings is None else testing_app_settings.us_auth_mode_override

            us_auth_mode = USAuthMode.master if us_auth_mode_override is None else us_auth_mode_override
        elif app_settings.APP_TYPE == AppType.DATA_CLOUD:
            yc_auth_settings = app_settings.YC_AUTH_SETTINGS
            assert yc_auth_settings is not None, "app_settings.YC_AUTH_SETTINGS must not be None with AppType.CLOUD"

            FlaskYCAuthService(
                authorization_mode=AuthorizationModeDataCloud(
                    project_permission_to_check=yc_auth_settings.YC_AUTHORIZE_PERMISSION,
                ),
                enable_cookie_auth=False,
                access_service_cfg=make_default_yc_auth_service_config(yc_auth_settings.YC_AS_ENDPOINT),
            ).set_up(app)
            us_auth_mode = USAuthMode.regular
        else:
            raise AssertionError(f"AppType not supported here: {app_settings.APP_TYPE!r}")

        result = EnvSetupResult(us_auth_mode=us_auth_mode)
        return result

    def create_app(
            self,
            app_settings: ControlPlaneAppSettings,
            testing_app_settings: Optional[ControlPlaneAppTestingsSettings] = None,
            close_loop_after_request: bool = True,
    ) -> flask.Flask:

        app = Flask(__name__)
        self.push_settings_to_flask_app_config(app, app_settings)

        TracingMiddleware(
            url_prefix_exclude=(
                '/ping',
                '/unistat',
                '/metrics',
            ),
        ).wrap_flask_app(app)
        ContextVarMiddleware().wrap_flask_app(app)

        load_bi_api_lib()

        if close_loop_after_request:
            AIOEventLoopMiddleware().wrap_flask_app(app)

        app.url_map.strict_slashes = False

        # To avoid error messages replacement by flask_restx
        app.config["ERROR_404_HELP"] = False
        app.config["SWAGGER_UI_DOC_EXPANSION"] = 'list'

        RequestLoggingContextControllerMiddleWare().set_up(app)
        TracingContextMiddleware().set_up(app)
        RequestIDService(
            request_id_app_prefix=app_settings.app_prefix,
        ).set_up(app)
        profiling_middleware.set_up(app, accept_outer_stages=False)

        # HAX to avoid bumping into inappropriate authorization added in 'before_request'.
        # TODO: move the inappropriate authorization to common handler wrap.
        register_ping_handler_hax(app)
        register_unistat_hax(app)
        register_metrics(app)

        env_setup_result = self.set_up_environment(
            app=app, app_settings=app_settings, testing_app_settings=testing_app_settings,
        )

        ReqCtxInfoMiddleware().set_up(app)

        connect_options_factory = ConnOptionsMutatorsFactory()

        def enable_index_fetching_mutator(
                conn_opts: ConnectOptions, conn: ExecutorBasedMixin
        ) -> Optional[ConnectOptions]:
            return conn_opts.clone(fetch_table_indexes=True)

        if app_settings.DO_DSRC_IDX_FETCH:
            connect_options_factory.add_mutator(enable_index_fetching_mutator)

        def set_use_manage_network_false_mutator(
                conn_opts: ConnectOptions, conn: ExecutorBasedMixin
        ) -> Optional[ConnectOptions]:
            return conn_opts.clone(use_managed_network=False)

        if app_settings.APP_TYPE == AppType.CLOUD and app_settings.MDB_FORCE_IGNORE_MANAGED_NETWORK:
            connect_options_factory.add_mutator(set_use_manage_network_false_mutator)

        sr_factory = self.get_sr_factory(
            settings=app_settings,
            conn_opts_factory=connect_options_factory
        )

        ServicesRegistryMiddleware(
            services_registry_factory=sr_factory,
        ).set_up(app)

        USManagerFlaskMiddleware(
            crypto_keys_config=app_settings.CRYPTO_KEYS_CONFIG,
            us_base_url=app_settings.US_BASE_URL,
            us_master_token=app_settings.US_MASTER_TOKEN,
            us_auth_mode=env_setup_result.us_auth_mode,
        ).set_up(app)

        getattr(app, 'logger')

        ma = Marshmallow()
        ma.init_app(app)

        init_apis(
            app,
            # TODO FIX: https://st.yandex-team.ru/BI-2708 move exception handling to common error-response formatter
            FlaskYCAuthService.get_flask_rest_plus_err_handler_map(),
        )

        return app
