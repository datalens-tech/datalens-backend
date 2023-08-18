from __future__ import annotations

from typing import Optional

import flask

from bi_constants.enums import USAuthMode
from bi_configs.enums import RequiredService, EnvType

from bi_core.data_processing.cache.primitives import CacheTTLConfig
from bi_core.services_registry.entity_checker import EntityUsageChecker
from bi_core.services_registry.env_manager_factory import CloudEnvManagerFactory
from bi_core.services_registry.env_manager_factory_base import EnvManagerFactory
from bi_core.services_registry.rqe_caches import RQECachesSetting
from bi_core.services_registry.sa_creds import SACredsSettings, SACredsRetrieverFactory

from bi_api_commons.yc_access_control_model import AuthorizationModeYandexCloud
from bi_api_commons_ya_cloud.flask.middlewares.yc_auth import FlaskYCAuthService
from bi_api_commons_ya_cloud.yc_auth import make_default_yc_auth_service_config
from bi_service_registry_ya_cloud.yc_service_registry import YCServiceRegistryFactory

from bi_api_lib.app_common import SRFactoryBuilder
from bi_api_lib.app.control_api.app import ControlApiAppFactory, EnvSetupResult
from bi_api_lib.app_settings import ControlPlaneAppSettings, ControlPlaneAppTestingsSettings, BaseAppSettings
from bi_api_lib.connector_availability.base import ConnectorAvailabilityConfig
from bi_api_lib.connector_availability.configs.development import CONFIG as DEVELOPMENT_CONNECTORS
from bi_api_lib.connector_availability.configs.ext_testing import CONFIG as EXT_TESTING_CONNECTORS  # TODO move inside this package
from bi_api_lib.connector_availability.configs.ext_production import CONFIG as EXT_PRODUCTION_CONNECTORS  # TODO -//-


class ControlApiSRFactoryBuilderYC(SRFactoryBuilder):
    def _get_required_services(self, settings: BaseAppSettings) -> set[RequiredService]:
        return set()

    def _get_env_manager_factory(self, settings: BaseAppSettings) -> EnvManagerFactory:
        return CloudEnvManagerFactory(samples_ch_hosts=list(settings.SAMPLES_CH_HOSTS))

    def _get_inst_specific_sr_factory(
            self,
            settings: BaseAppSettings,
    ) -> YCServiceRegistryFactory:
        sa_creds_settings = SACredsSettings(
            mode=settings.YC_SA_CREDS_MODE,
            env_key_data=settings.YC_SA_CREDS_KEY_DATA,
        ) if settings.YC_SA_CREDS_MODE is not None else None

        return YCServiceRegistryFactory(
            yc_billing_host=settings.YC_BILLING_HOST,
            yc_api_endpoint_rm=settings.YC_RM_CP_ENDPOINT,
            yc_as_endpoint=settings.YC_AUTH_SETTINGS.YC_AS_ENDPOINT if settings.YC_AUTH_SETTINGS else None,
            yc_api_endpoint_iam=settings.YC_AUTH_SETTINGS.YC_API_ENDPOINT_IAM if settings.YC_AUTH_SETTINGS else None,
            yc_ts_endpoint=settings.YC_IAM_TS_ENDPOINT,
            sa_creds_retriever_factory=SACredsRetrieverFactory(
                sa_creds_settings=sa_creds_settings,
                ts_endpoint=settings.YC_IAM_TS_ENDPOINT
            ) if sa_creds_settings else None
        )

    def _get_entity_usage_checker(self, settings: BaseAppSettings) -> Optional[EntityUsageChecker]:
        return None

    def _get_bleeding_edge_users(self, settings: BaseAppSettings) -> tuple[str, ...]:
        return tuple()

    def _get_rqe_caches_settings(self, settings: BaseAppSettings) -> Optional[RQECachesSetting]:
        return None

    def _get_default_cache_ttl_settings(self, settings: BaseAppSettings) -> Optional[CacheTTLConfig]:
        return None

    def _get_connector_availability(self, settings: BaseAppSettings) -> Optional[ConnectorAvailabilityConfig]:
        if (env_type := settings.ENV_TYPE) is not None:
            if env_type == EnvType.yc_testing:
                return EXT_TESTING_CONNECTORS
            elif env_type == EnvType.development:
                return DEVELOPMENT_CONNECTORS
        return EXT_PRODUCTION_CONNECTORS


class ControlApiAppFactoryYC(ControlApiAppFactory, ControlApiSRFactoryBuilderYC):
    def push_settings_to_flask_app_config(
            self,
            target_app: flask.Flask,
            app_settings: ControlPlaneAppSettings,
    ) -> None:
        cfg = target_app.config
        super().push_settings_to_flask_app_config(target_app, app_settings)

        if app_settings.YC_AUTH_SETTINGS:
            cfg["YC_AUTHORIZE_PERMISSION"] = app_settings.YC_AUTH_SETTINGS.YC_AUTHORIZE_PERMISSION
            cfg["YC_AS_ENDPOINT"] = app_settings.YC_AUTH_SETTINGS.YC_AS_ENDPOINT

        cfg["YC_TS_ENDPOINT"] = app_settings.YC_IAM_TS_ENDPOINT

        cfg["CLOUD_API_IAM_ENDPOINT"] = app_settings.YC_IAM_CP_ENDPOINT
        cfg["CLOUD_API_RM_ENDPOINT"] = app_settings.YC_RM_CP_ENDPOINT
        cfg["YC_BILLING_HOST"] = app_settings.YC_BILLING_HOST

    def set_up_environment(
            self,
            app: flask.Flask, app_settings: ControlPlaneAppSettings,
            testing_app_settings: Optional[ControlPlaneAppTestingsSettings] = None,
    ) -> EnvSetupResult:
        us_auth_mode: USAuthMode
        yc_auth_settings = app_settings.YC_AUTH_SETTINGS
        assert yc_auth_settings is not None, "app_settings.YC_AUTH_SETTINGS must not be None"

        FlaskYCAuthService(
            authorization_mode=AuthorizationModeYandexCloud(
                folder_permission_to_check=yc_auth_settings.YC_AUTHORIZE_PERMISSION,
                organization_permission_to_check=yc_auth_settings.YC_AUTHORIZE_PERMISSION,
            ),
            enable_cookie_auth=False,
            access_service_cfg=make_default_yc_auth_service_config(yc_auth_settings.YC_AS_ENDPOINT),
        ).set_up(app)
        us_auth_mode = USAuthMode.regular

        result = EnvSetupResult(us_auth_mode=us_auth_mode)
        return result
