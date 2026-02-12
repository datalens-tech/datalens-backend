from __future__ import annotations

import abc
from typing import (
    TYPE_CHECKING,
    Generic,
    TypeVar,
)

import attr
import flask
from flask import Flask
from flask_marshmallow import Marshmallow

from dl_api_commons.flask.middlewares.aio_event_loop_middleware import AIOEventLoopMiddleware
from dl_api_commons.flask.middlewares.commit_rci_middleware import ReqCtxInfoMiddleware
from dl_api_commons.flask.middlewares.context_var_middleware import ContextVarMiddleware
from dl_api_commons.flask.middlewares.logging_context import RequestLoggingContextControllerMiddleWare
from dl_api_commons.flask.middlewares.obfuscation_context import setup_obfuscation_context_middleware
from dl_api_commons.flask.middlewares.request_id import RequestIDService
from dl_api_commons.flask.middlewares.tracing import (
    TracingContextMiddleware,
    TracingMiddleware,
)
from dl_api_lib.app.control_api.resources import init_apis
from dl_api_lib.app_common import SRFactoryBuilder
from dl_api_lib.app_common_settings import ConnOptionsMutatorsFactory
from dl_api_lib.app_settings import (
    ControlApiAppSettings,
    ControlApiAppTestingsSettings,
)
from dl_configs.utils import get_multiple_root_certificates
from dl_constants.enums import USAuthMode
from dl_core import profiling_middleware
from dl_core.connectors.settings.base import ConnectorSettings
from dl_core.flask_utils.services_registry_middleware import ServicesRegistryMiddleware
from dl_core.flask_utils.us_manager_middleware import USManagerFlaskMiddleware
from dl_obfuscator import (
    OBFUSCATION_ENGINE_KEY,
    SecretKeeper,
    create_obfuscation_engine,
)
import dl_retrier


if TYPE_CHECKING:
    from dl_core.connection_models import ConnectOptions
    from dl_core.us_connection_base import ConnectionBase


@attr.s(frozen=True)
class EnvSetupResult:
    us_auth_mode: USAuthMode = attr.ib(kw_only=True)


TControlApiAppSettings = TypeVar("TControlApiAppSettings", bound=ControlApiAppSettings)


@attr.s(kw_only=True)
class ControlApiAppFactory(SRFactoryBuilder, Generic[TControlApiAppSettings], abc.ABC):
    _settings: TControlApiAppSettings = attr.ib()

    @abc.abstractmethod
    def set_up_environment(
        self,
        app: flask.Flask,
        testing_app_settings: ControlApiAppTestingsSettings | None = None,
    ) -> EnvSetupResult:
        raise NotImplementedError()

    @property
    def _is_async_env(self) -> bool:
        return False

    def _get_us_manager_middleware(
        self,
        us_auth_mode: USAuthMode,
        ca_data: bytes,
    ) -> USManagerFlaskMiddleware:
        return USManagerFlaskMiddleware(
            crypto_keys_config=self._settings.CRYPTO_KEYS_CONFIG,
            us_base_url=self._settings.US_BASE_URL,
            us_master_token=self._settings.US_MASTER_TOKEN,
            us_auth_mode=us_auth_mode,
            ca_data=ca_data,
            retry_policy_factory=dl_retrier.RetryPolicyFactory.from_settings(self._settings.US_CLIENT.RETRY_POLICY),
        )

    def _get_conn_opts_mutators_factory(self) -> ConnOptionsMutatorsFactory:
        conn_opts_mutators_factory = ConnOptionsMutatorsFactory()

        def enable_index_fetching_mutator(conn_opts: ConnectOptions, conn: ConnectionBase) -> ConnectOptions | None:
            return conn_opts.clone(fetch_table_indexes=True)

        if self._settings.DO_DSRC_IDX_FETCH:
            conn_opts_mutators_factory.add_mutator(enable_index_fetching_mutator)

        return conn_opts_mutators_factory

    def create_app(
        self,
        connectors_settings: dict[str, ConnectorSettings],
        testing_app_settings: ControlApiAppTestingsSettings | None = None,
        close_loop_after_request: bool = True,
    ) -> flask.Flask:
        app = Flask(__name__)

        TracingMiddleware(
            url_prefix_exclude=(
                "/api/v1/ping",
                "/api/v1/unistat",
                "/api/v1/metrics",
            ),
        ).wrap_flask_app(app)
        ContextVarMiddleware().wrap_flask_app(app)

        ca_data = get_multiple_root_certificates(
            self._settings.CA_FILE_PATH,
            *self._settings.EXTRA_CA_FILE_PATHS,
        )

        if close_loop_after_request:
            AIOEventLoopMiddleware().wrap_flask_app(app)

        app.url_map.strict_slashes = False

        # To avoid error messages replacement by flask_restx
        app.config["ERROR_404_HELP"] = False
        app.config["SWAGGER_UI_DOC_EXPANSION"] = "list"

        RequestLoggingContextControllerMiddleWare().set_up(app)
        TracingContextMiddleware().set_up(app)
        RequestIDService(
            request_id_app_prefix=self._settings.app_prefix,
        ).set_up(app)
        profiling_middleware.set_up(app, accept_outer_stages=False)

        env_setup_result = self.set_up_environment(app=app, testing_app_settings=testing_app_settings)

        ReqCtxInfoMiddleware().set_up(app)

        if self._settings.OBFUSCATION_ENABLED:
            global_keeper = SecretKeeper()
            if self._settings.US_MASTER_TOKEN:
                # just for example for now. More secrets will be added in BI-6492
                global_keeper.add_secret(self._settings.US_MASTER_TOKEN, "us_master_token")
            app.config[OBFUSCATION_ENGINE_KEY] = create_obfuscation_engine(global_keeper=global_keeper)
        setup_obfuscation_context_middleware(app)

        conn_opts_mutators_factory = self._get_conn_opts_mutators_factory()

        sr_factory = self.get_sr_factory(
            settings=self._settings,
            conn_opts_factory=conn_opts_mutators_factory,
            connectors_settings=connectors_settings,
            ca_data=ca_data,
        )

        ServicesRegistryMiddleware(
            services_registry_factory=sr_factory,
        ).set_up(app)

        self._get_us_manager_middleware(
            us_auth_mode=env_setup_result.us_auth_mode,
            ca_data=ca_data,
        ).set_up(app)

        _ = app.logger

        ma = Marshmallow()
        ma.init_app(app)

        init_apis(app)

        return app
