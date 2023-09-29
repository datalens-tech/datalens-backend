from __future__ import annotations

from typing import Optional

from data_api import app_version

from bi_api_commons_ya_team.aio.middlewares.blackbox_auth import blackbox_auth_middleware
from bi_api_lib_ya.app_settings import DataApiAppSettingsYT
from bi_api_lib_ya.connections_security.base import MDBDomainManagerSettings
from bi_api_lib_ya.services_registry.env_manager_factory import IntranetEnvManagerFactory
from bi_service_registry_ya_team.yt_service_registry import YTServiceRegistryFactory
from dl_api_commons.aio.typing import AIOHTTPMiddleware
from dl_api_lib.app.data_api.app import (
    DataApiAppFactory,
    EnvSetupResult,
)
from dl_api_lib.app_common import SRFactoryBuilder
from dl_api_lib.app_common_settings import ConnOptionsMutatorsFactory
from dl_api_lib.connector_availability.base import ConnectorAvailabilityConfig
from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_configs.enums import RequiredService
from dl_constants.enums import ConnectionType
from dl_core.aio.middlewares.services_registry import services_registry_middleware
from dl_core.aio.middlewares.us_manager import (
    service_us_manager_middleware,
    us_manager_middleware,
)
from dl_core.connection_models import ConnectOptions
from dl_core.data_processing.cache.primitives import CacheTTLConfig
from dl_core.services_registry.entity_checker import EntityUsageChecker
from dl_core.services_registry.env_manager_factory_base import EnvManagerFactory
from dl_core.services_registry.inst_specific_sr import InstallationSpecificServiceRegistryFactory
from dl_core.services_registry.rqe_caches import RQECachesSetting
from dl_core.us_connection_base import ExecutorBasedMixin

from bi_connector_chyt_internal.core.dto import CHYTInternalBaseDTO
from bi_connector_chyt_internal.core.us_connection import BaseConnectionCHYTInternal


class DataApiSRFactoryBuilderYT(SRFactoryBuilder[DataApiAppSettingsYT]):
    @property
    def _is_async_env(self) -> bool:
        return True

    def _get_required_services(self, settings: DataApiAppSettingsYT) -> set[RequiredService]:
        required_services: set[RequiredService] = {
            RequiredService.RQE_INT_SYNC,
        }
        return required_services

    def _get_env_manager_factory(self, settings: DataApiAppSettingsYT) -> EnvManagerFactory:
        mdb_domain_manager_settings = MDBDomainManagerSettings(
            managed_network_enabled=settings.MDB.MANAGED_NETWORK_ENABLED,
            mdb_domains=settings.MDB.DOMAINS,
            mdb_cname_domains=settings.MDB.CNAME_DOMAINS,
            renaming_map=settings.MDB.MANAGED_NETWORK_REMAP,
        )
        return IntranetEnvManagerFactory(mdb_domain_manager_settings=mdb_domain_manager_settings)

    def _get_inst_specific_sr_factory(
        self,
        settings: DataApiAppSettingsYT,
    ) -> Optional[InstallationSpecificServiceRegistryFactory]:
        return YTServiceRegistryFactory(
            dls_host=settings.DLS_HOST,
            dls_api_key=settings.DLS_API_KEY,
        )

    def _get_entity_usage_checker(self, settings: DataApiAppSettingsYT) -> Optional[EntityUsageChecker]:
        return None

    def _get_bleeding_edge_users(self, settings: DataApiAppSettingsYT) -> tuple[str, ...]:
        return settings.BLEEDING_EDGE_USERS

    def _get_rqe_caches_settings(self, settings: DataApiAppSettingsYT) -> Optional[RQECachesSetting]:
        return None

    def _get_default_cache_ttl_settings(self, settings: DataApiAppSettingsYT) -> Optional[CacheTTLConfig]:
        if settings.CACHES_TTL_SETTINGS is not None:
            # FIXME: resolve typing mismatch for CacheTTLConfig
            return CacheTTLConfig(
                ttl_sec_direct=settings.CACHES_TTL_SETTINGS.OTHER,  # type: ignore  # TODO: fix
                ttl_sec_materialized=settings.CACHES_TTL_SETTINGS.MATERIALIZED,  # type: ignore  # TODO: fix
            )
        return None

    def _get_connector_availability(self, settings: DataApiAppSettingsYT) -> Optional[ConnectorAvailabilityConfig]:
        return None


class DataApiAppFactoryYT(DataApiAppFactory[DataApiAppSettingsYT], DataApiSRFactoryBuilderYT):
    @property
    def _is_public(self) -> bool:
        return False

    def get_app_version(self) -> str:
        return app_version

    def set_up_environment(
        self,
        connectors_settings: dict[ConnectionType, ConnectorSettingsBase],
    ) -> EnvSetupResult:
        auth_mw_list: list[AIOHTTPMiddleware]
        sr_middleware_list: list[AIOHTTPMiddleware]
        usm_middleware_list: list[AIOHTTPMiddleware]

        conn_opts_factory = ConnOptionsMutatorsFactory()
        sr_factory = self.get_sr_factory(
            settings=self._settings, conn_opts_factory=conn_opts_factory, connectors_settings=connectors_settings
        )

        auth_mw_list = [
            blackbox_auth_middleware(),
        ]

        def chyt_mirroring_conn_opts_mutator(
            conn_opts: ConnectOptions, conn: ExecutorBasedMixin
        ) -> Optional[ConnectOptions]:
            if not isinstance(conn, BaseConnectionCHYTInternal):
                return None
            mirroring_config = self._settings.CHYT_MIRRORING

            if mirroring_config is None:
                return None
            conn_dto: CHYTInternalBaseDTO = conn.get_conn_dto()  # type: ignore
            cluster_name = conn_dto.cluster.lower()
            mirroring_clique_alias = mirroring_config.MAP.get(
                (cluster_name, conn_dto.clique_alias)
            ) or mirroring_config.MAP.get((cluster_name, None))
            if not mirroring_clique_alias:
                return None
            return conn_opts.clone(
                mirroring_frac=mirroring_config.FRAC,
                mirroring_clique_req_timeout_sec=mirroring_config.REQ_TIMEOUT_SEC,
                mirroring_clique_alias=mirroring_clique_alias,
            )

        conn_opts_factory.add_mutator(chyt_mirroring_conn_opts_mutator)

        sr_middleware_list = [
            services_registry_middleware(
                services_registry_factory=sr_factory,
                use_query_cache=self._settings.CACHES_ON,
                use_mutation_cache=self._settings.MUTATIONS_CACHES_ON,
                mutation_cache_default_ttl=self._settings.MUTATIONS_CACHES_DEFAULT_TTL,
            ),
        ]

        common_us_kw = dict(
            us_base_url=self._settings.US_BASE_URL,
            crypto_keys_config=self._settings.CRYPTO_KEYS_CONFIG,
        )
        usm_middleware_list = [
            us_manager_middleware(**common_us_kw),  # type: ignore
            service_us_manager_middleware(us_master_token=self._settings.US_MASTER_TOKEN, **common_us_kw),  # type: ignore
        ]

        result = EnvSetupResult(
            auth_mw_list=auth_mw_list,
            sr_middleware_list=sr_middleware_list,
            usm_middleware_list=usm_middleware_list,
        )

        return result
