from __future__ import annotations

import logging
import ssl
from typing import Optional

from dl_api_commons.aio.middlewares.auth_trust_middleware import auth_trust_middleware
from dl_api_commons.aio.typing import AIOHTTPMiddleware
from dl_api_lib.app.data_api.app import (
    DataApiAppFactory,
    EnvSetupResult,
)
from dl_api_lib.app_common import SRFactoryBuilder
from dl_api_lib.app_common_settings import ConnOptionsMutatorsFactory
from dl_api_lib.app_settings import (
    AppSettings,
    DataApiAppSettingsOS,
)
from dl_api_lib.connector_availability.base import ConnectorAvailabilityConfig
from dl_cache_engine.primitives import CacheTTLConfig
from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_configs.enums import RequiredService
from dl_configs.utils import get_multiple_root_certificates
from dl_constants.enums import ConnectionType
from dl_core.aio.middlewares.services_registry import services_registry_middleware
from dl_core.aio.middlewares.us_manager import (
    service_us_manager_middleware,
    us_manager_middleware,
)
from dl_core.services_registry.entity_checker import EntityUsageChecker
from dl_core.services_registry.env_manager_factory import InsecureEnvManagerFactory
from dl_core.services_registry.env_manager_factory_base import EnvManagerFactory
from dl_core.services_registry.inst_specific_sr import InstallationSpecificServiceRegistryFactory
from dl_core.services_registry.rqe_caches import RQECachesSetting
from dl_data_api import app_version


LOGGER = logging.getLogger(__name__)


class StandaloneDataApiSRFactoryBuilder(SRFactoryBuilder[AppSettings]):
    @property
    def _is_async_env(self) -> bool:
        return True

    def _get_required_services(self, settings: AppSettings) -> set[RequiredService]:
        return set()

    def _get_env_manager_factory(self, settings: AppSettings) -> EnvManagerFactory:
        return InsecureEnvManagerFactory()

    def _get_inst_specific_sr_factory(
        self,
        settings: AppSettings,
        ca_data: bytes,
    ) -> Optional[InstallationSpecificServiceRegistryFactory]:
        return None

    def _get_entity_usage_checker(self, settings: AppSettings) -> Optional[EntityUsageChecker]:
        return None

    def _get_bleeding_edge_users(self, settings: AppSettings) -> tuple[str, ...]:
        return tuple()

    def _get_rqe_caches_settings(self, settings: DataApiAppSettingsOS) -> Optional[RQECachesSetting]:  # type: ignore[override]
        return None

    def _get_default_cache_ttl_settings(self, settings: DataApiAppSettingsOS) -> Optional[CacheTTLConfig]:  # type: ignore[override]
        return None

    def _get_connector_availability(self, settings: AppSettings) -> Optional[ConnectorAvailabilityConfig]:
        return None


class StandaloneDataApiAppFactory(DataApiAppFactory[DataApiAppSettingsOS], StandaloneDataApiSRFactoryBuilder):
    @property
    def _is_public(self) -> bool:
        return False

    def get_app_version(self) -> str:
        return app_version

    def set_up_environment(
        self,
        connectors_settings: dict[ConnectionType, ConnectorSettingsBase],
    ) -> EnvSetupResult:
        sr_middleware_list: list[AIOHTTPMiddleware]
        usm_middleware_list: list[AIOHTTPMiddleware]

        ca_data = get_multiple_root_certificates(
            self._settings.CA_FILE_PATH,
            *self._settings.EXTRA_CA_FILE_PATHS,
        )

        conn_opts_factory = ConnOptionsMutatorsFactory()
        sr_factory = self.get_sr_factory(
            settings=self._settings,
            conn_opts_factory=conn_opts_factory,
            connectors_settings=connectors_settings,
            ca_data=ca_data,
        )

        # Auth middlewares
        auth_mw = self._get_auth_middleware()

        # SR middlewares
        sr_middleware_list = [
            services_registry_middleware(
                services_registry_factory=sr_factory,
                use_query_cache=self._settings.CACHES_ON,
                use_mutation_cache=self._settings.MUTATIONS_CACHES_ON,
                mutation_cache_default_ttl=self._settings.MUTATIONS_CACHES_DEFAULT_TTL,
            ),
        ]

        # US manager middlewares
        common_us_kw = dict(
            us_base_url=self._settings.US_BASE_URL,
            crypto_keys_config=self._settings.CRYPTO_KEYS_CONFIG,
            ca_data=ca_data,
        )

        if self._settings.AUTH is not None and self._settings.AUTH == "NONE":
            usm_middleware_list = [
                service_us_manager_middleware(us_master_token=self._settings.US_MASTER_TOKEN, **common_us_kw),  # type: ignore  # 2024-01-30 # TODO: Argument "us_master_token" to "service_us_manager_middleware" has incompatible type "str | None"; expected "str"  [arg-type]
                service_us_manager_middleware(us_master_token=self._settings.US_MASTER_TOKEN, as_user_usm=True, **common_us_kw),  # type: ignore  # 2024-01-30 # TODO: Argument "us_master_token" to "service_us_manager_middleware" has incompatible type "str | None"; expected "str"  [arg-type]
            ]
        else:
            usm_middleware_list = [
                us_manager_middleware(**common_us_kw),  # type: ignore
                service_us_manager_middleware(us_master_token=self._settings.US_MASTER_TOKEN, **common_us_kw),  # type: ignore
            ]

        result = EnvSetupResult(
            auth_mw_list=[auth_mw],
            sr_middleware_list=sr_middleware_list,
            usm_middleware_list=usm_middleware_list,
        )

        return result

    def _get_auth_middleware(self) -> AIOHTTPMiddleware:
        if self._settings.AUTH is None or self._settings.AUTH.TYPE == "NONE":
            return self._get_auth_middleware_none()

        if self._settings.AUTH.TYPE == "ZITADEL":
            return self._get_auth_middleware_zitadel(
                ca_data=get_multiple_root_certificates(
                    self._settings.CA_FILE_PATH,
                    *self._settings.EXTRA_CA_FILE_PATHS,
                ),
            )

        raise ValueError(f"Unknown auth type: {self._settings.AUTH.TYPE}")

    def _get_auth_middleware_none(
        self,
    ) -> AIOHTTPMiddleware:
        return auth_trust_middleware(
            fake_user_id="_user_id_",
            fake_user_name="_user_name_",
        )

    def _get_auth_middleware_zitadel(
        self,
        ca_data: bytes,
    ) -> AIOHTTPMiddleware:
        self._settings: DataApiAppSettingsOS
        assert self._settings.AUTH is not None

        import httpx

        import dl_zitadel

        ca_data = get_multiple_root_certificates(
            self._settings.CA_FILE_PATH,
            *self._settings.EXTRA_CA_FILE_PATHS,
        )

        zitadel_client = dl_zitadel.ZitadelAsyncClient(
            base_client=httpx.AsyncClient(
                verify=ssl.create_default_context(cadata=ca_data.decode("ascii")),
            ),
            base_url=self._settings.AUTH.BASE_URL,
            project_id=self._settings.AUTH.PROJECT_ID,
            client_id=self._settings.AUTH.CLIENT_ID,
            client_secret=self._settings.AUTH.CLIENT_SECRET,
            app_client_id=self._settings.AUTH.APP_CLIENT_ID,
            app_client_secret=self._settings.AUTH.APP_CLIENT_SECRET,
        )
        token_storage = dl_zitadel.ZitadelAsyncTokenStorage(
            client=zitadel_client,
        )
        middleware = dl_zitadel.AioHTTPMiddleware(
            client=zitadel_client,
            token_storage=token_storage,
        )

        return middleware.get_middleware()
