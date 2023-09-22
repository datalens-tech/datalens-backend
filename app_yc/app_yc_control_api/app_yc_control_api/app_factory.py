from __future__ import annotations

from typing import Optional

from app_yc_control_api.i18n.localizer import CONFIGS
import flask

from bi_api_commons_ya_cloud.flask.middlewares.yc_auth import FlaskYCAuthService
from bi_api_commons_ya_cloud.yc_access_control_model import AuthorizationModeYandexCloud
from bi_api_commons_ya_cloud.yc_auth import make_default_yc_auth_service_config
from bi_api_lib_ya.app_settings import ControlPlaneAppSettings
from bi_api_lib_ya.connections_security.base import MDBDomainManagerSettings
from bi_api_lib_ya.services_registry.env_manager_factory import CloudEnvManagerFactory
from bi_cloud_integration.sa_creds import (
    SACredsRetrieverFactory,
    SACredsSettings,
)
from bi_service_registry_ya_cloud.yc_service_registry import YCServiceRegistryFactory
from dl_api_lib.app.control_api.app import (
    ControlApiAppFactory,
    EnvSetupResult,
)
from dl_api_lib.app_common import SRFactoryBuilder
from dl_api_lib.app_common_settings import ConnOptionsMutatorsFactory
from dl_api_lib.app_settings import ControlApiAppTestingsSettings
from dl_api_lib.connector_availability.base import ConnectorAvailabilityConfig
from dl_configs.enums import RequiredService
from dl_constants.enums import USAuthMode
from dl_core.connection_models import ConnectOptions
from dl_core.data_processing.cache.primitives import CacheTTLConfig
from dl_core.services_registry.entity_checker import EntityUsageChecker
from dl_core.services_registry.env_manager_factory_base import EnvManagerFactory
from dl_core.services_registry.rqe_caches import RQECachesSetting
from dl_core.us_connection_base import ExecutorBasedMixin
from dl_i18n.localizer_base import TranslationConfig

from bi_connector_yql.core.ydb.us_connection import YDBConnectOptions


class ControlApiSRFactoryBuilderYC(SRFactoryBuilder[ControlPlaneAppSettings]):
    def _get_required_services(self, settings: ControlPlaneAppSettings) -> set[RequiredService]:
        return set()

    def _get_env_manager_factory(self, settings: ControlPlaneAppSettings) -> EnvManagerFactory:
        mdb_domain_manager_settings = MDBDomainManagerSettings(
            managed_network_enabled=settings.MDB.MANAGED_NETWORK_ENABLED,
            mdb_domains=settings.MDB.DOMAINS,
            mdb_cname_domains=settings.MDB.CNAME_DOMAINS,
            renaming_map=settings.MDB.MANAGED_NETWORK_REMAP,
        )
        return CloudEnvManagerFactory(
            mdb_domain_manager_settings=mdb_domain_manager_settings,
            samples_ch_hosts=list(settings.SAMPLES_CH_HOSTS),
        )

    def _get_inst_specific_sr_factory(
        self,
        settings: ControlPlaneAppSettings,
    ) -> YCServiceRegistryFactory:
        sa_creds_settings = (
            SACredsSettings(
                mode=settings.YC_SA_CREDS_MODE,
                env_key_data=settings.YC_SA_CREDS_KEY_DATA,
            )
            if settings.YC_SA_CREDS_MODE is not None
            else None
        )

        return YCServiceRegistryFactory(
            yc_billing_host=settings.YC_BILLING_HOST,
            yc_api_endpoint_rm=settings.YC_RM_CP_ENDPOINT,
            yc_as_endpoint=settings.YC_AUTH_SETTINGS.YC_AS_ENDPOINT if settings.YC_AUTH_SETTINGS else None,
            yc_api_endpoint_iam=settings.YC_AUTH_SETTINGS.YC_API_ENDPOINT_IAM if settings.YC_AUTH_SETTINGS else None,
            yc_ts_endpoint=settings.YC_IAM_TS_ENDPOINT,
            yc_api_endpoint_mdb=settings.YC_MDB_API_ENDPOINT,
            sa_creds_retriever_factory=SACredsRetrieverFactory(
                sa_creds_settings=sa_creds_settings, ts_endpoint=settings.YC_IAM_TS_ENDPOINT
            )
            if sa_creds_settings
            else None,
        )

    def _get_entity_usage_checker(self, settings: ControlPlaneAppSettings) -> Optional[EntityUsageChecker]:
        return None

    def _get_bleeding_edge_users(self, settings: ControlPlaneAppSettings) -> tuple[str, ...]:
        return tuple()

    def _get_rqe_caches_settings(self, settings: ControlPlaneAppSettings) -> Optional[RQECachesSetting]:
        return None

    def _get_default_cache_ttl_settings(self, settings: ControlPlaneAppSettings) -> Optional[CacheTTLConfig]:
        return None

    def _get_connector_availability(self, settings: ControlPlaneAppSettings) -> Optional[ConnectorAvailabilityConfig]:
        return settings.CONNECTOR_AVAILABILITY

    @property
    def _extra_translation_configs(self) -> set[TranslationConfig]:
        configs = super()._extra_translation_configs
        configs.update(CONFIGS)
        return configs


class ControlApiAppFactoryYC(ControlApiAppFactory[ControlPlaneAppSettings], ControlApiSRFactoryBuilderYC):
    def _get_conn_opts_mutators_factory(self) -> ConnOptionsMutatorsFactory:
        conn_opts_mutators_factory = super()._get_conn_opts_mutators_factory()

        def ydb_is_cloud_mutator(conn_opts: ConnectOptions, conn: ExecutorBasedMixin) -> Optional[ConnectOptions]:
            if isinstance(conn_opts, YDBConnectOptions):
                return conn_opts.clone(is_cloud=True)
            return None

        conn_opts_mutators_factory.add_mutator(ydb_is_cloud_mutator)
        return conn_opts_mutators_factory

    def set_up_environment(
        self,
        app: flask.Flask,
        testing_app_settings: Optional[ControlApiAppTestingsSettings] = None,
    ) -> EnvSetupResult:
        us_auth_mode: USAuthMode
        yc_auth_settings = self._settings.YC_AUTH_SETTINGS
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
