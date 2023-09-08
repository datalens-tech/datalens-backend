from __future__ import annotations

import abc
from typing import Optional

import flask

from bi_configs.enums import AppType
from bi_constants.enums import USAuthMode

from bi_api_lib.app.control_api.app import ControlApiAppFactory, EnvSetupResult
from bi_api_lib.app_common_settings import ConnOptionsMutatorsFactory
from bi_api_lib.app_settings import ControlApiAppTestingsSettings
from bi_api_lib_ya.app_settings import ControlPlaneAppSettings

from bi_core.connection_models import ConnectOptions
from bi_core.us_connection_base import ExecutorBasedMixin

from bi_api_commons_ya_cloud.flask.middlewares.yc_auth import FlaskYCAuthService
from bi_api_commons_ya_cloud.yc_access_control_model import AuthorizationModeYandexCloud, AuthorizationModeDataCloud
from bi_api_commons_ya_cloud.yc_auth import make_default_yc_auth_service_config


class LegacyControlApiAppFactory(ControlApiAppFactory[ControlPlaneAppSettings], abc.ABC):
    def _get_conn_opts_mutators_factory(self) -> ConnOptionsMutatorsFactory:
        conn_opts_mutators_factory = super()._get_conn_opts_mutators_factory()

        def set_use_manage_network_false_mutator(
                conn_opts: ConnectOptions, conn: ExecutorBasedMixin
        ) -> Optional[ConnectOptions]:
            return conn_opts.clone(use_managed_network=False)

        if self._settings.APP_TYPE == AppType.CLOUD and self._settings.MDB_FORCE_IGNORE_MANAGED_NETWORK:
            conn_opts_mutators_factory.add_mutator(set_use_manage_network_false_mutator)

        return conn_opts_mutators_factory

    def set_up_environment(
            self,
            app: flask.Flask,
            testing_app_settings: Optional[ControlApiAppTestingsSettings] = None,
    ) -> EnvSetupResult:
        us_auth_mode: USAuthMode
        if self._settings.APP_TYPE == AppType.INTRANET:
            from bi_api_commons_ya_team.flask.middlewares import blackbox_auth
            blackbox_auth.set_up(app, self._settings.BLACKBOX_RETRY_PARAMS, self._settings.BLACKBOX_TIMEOUT)
            us_auth_mode = USAuthMode.regular
        elif self._settings.APP_TYPE in (AppType.CLOUD, AppType.NEBIUS):
            yc_auth_settings = self._settings.YC_AUTH_SETTINGS
            assert yc_auth_settings is not None, "settings.YC_AUTH_SETTINGS must not be None with AppType.CLOUD"

            FlaskYCAuthService(
                authorization_mode=AuthorizationModeYandexCloud(
                    folder_permission_to_check=yc_auth_settings.YC_AUTHORIZE_PERMISSION,
                    organization_permission_to_check=yc_auth_settings.YC_AUTHORIZE_PERMISSION,
                ),
                enable_cookie_auth=False,
                access_service_cfg=make_default_yc_auth_service_config(yc_auth_settings.YC_AS_ENDPOINT),
            ).set_up(app)
            us_auth_mode = USAuthMode.regular
        elif self._settings.APP_TYPE == AppType.TESTS:
            from bi_core.flask_utils.trust_auth import TrustAuthService
            TrustAuthService(
                fake_user_id='_the_tests_syncapp_user_id_',
                fake_user_name='_the_tests_syncapp_user_name_',
                fake_tenant=None if testing_app_settings is None else testing_app_settings.fake_tenant
            ).set_up(app)

            us_auth_mode_override = None if testing_app_settings is None else testing_app_settings.us_auth_mode_override

            us_auth_mode = USAuthMode.master if us_auth_mode_override is None else us_auth_mode_override
        elif self._settings.APP_TYPE == AppType.DATA_CLOUD:
            yc_auth_settings = self._settings.YC_AUTH_SETTINGS
            assert yc_auth_settings is not None, "settings.YC_AUTH_SETTINGS must not be None with AppType.CLOUD"

            FlaskYCAuthService(
                authorization_mode=AuthorizationModeDataCloud(
                    project_permission_to_check=yc_auth_settings.YC_AUTHORIZE_PERMISSION,
                ),
                enable_cookie_auth=False,
                access_service_cfg=make_default_yc_auth_service_config(yc_auth_settings.YC_AS_ENDPOINT),
            ).set_up(app)
            us_auth_mode = USAuthMode.regular
        else:
            raise AssertionError(f"AppType not supported here: {self._settings.APP_TYPE!r}")

        result = EnvSetupResult(us_auth_mode=us_auth_mode)
        return result
