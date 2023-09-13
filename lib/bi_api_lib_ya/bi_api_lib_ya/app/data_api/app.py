from __future__ import annotations

import abc
from typing import Optional

from bi_configs.connectors_settings import ConnectorSettingsBase
from bi_configs.enums import AppType
from bi_constants.api_constants import YcTokenHeaderMode
from bi_constants.enums import ConnectionType

from bi_core.aio.middlewares.auth_trust_middleware import auth_trust_middleware
from bi_core.aio.middlewares.services_registry import services_registry_middleware
from bi_core.aio.middlewares.us_manager import (
    public_usm_workaround_middleware,
    public_us_manager_middleware,
    service_us_manager_middleware,
    us_manager_middleware,
)
from bi_core.connection_models import ConnectOptions
from bi_core.us_connection_base import ExecutorBasedMixin

from bi_api_lib.aio.middlewares.public_api_key_middleware import public_api_key_middleware
from bi_api_lib.app.data_api.app import DataApiAppFactory, EnvSetupResult
from bi_api_lib.app_common_settings import ConnOptionsMutatorsFactory
from bi_api_lib_ya.app_settings import AsyncAppSettings

from bi_api_commons.aio.typing import AIOHTTPMiddleware
from bi_api_commons_ya_cloud.aio.middlewares.yc_auth import YCAuthService, YCEmbedAuthService
from bi_api_commons_ya_cloud.tenant_resolver import TenantResolverYC
from bi_api_commons_ya_cloud.yc_access_control_model import AuthorizationModeYandexCloud, AuthorizationModeDataCloud
from bi_api_commons_ya_cloud.yc_auth import make_default_yc_auth_service_config
from bi_api_commons_ya_team.aio.middlewares.blackbox_auth import blackbox_auth_middleware

# TODO: remove dependencies on connectors
from bi_connector_chyt_internal.core.us_connection import BaseConnectionCHYTInternal
from bi_connector_clickhouse.core.clickhouse.us_connection import ConnectionClickhouse
from bi_connector_yql.core.ydb.us_connection import YDBConnectOptions


class LegacyDataApiAppFactory(DataApiAppFactory[AsyncAppSettings], abc.ABC):
    @property
    def _is_public(self) -> bool:
        return self._settings.APP_TYPE in (AppType.CLOUD_PUBLIC, AppType.CLOUD_EMBED, AppType.DATA_CLOUD_EMBED)

    def set_up_environment(
            self,
            connectors_settings: dict[ConnectionType, ConnectorSettingsBase],
    ) -> EnvSetupResult:
        # TODO: Move the rest of the env-dependent stuff here

        auth_mw_list: list[AIOHTTPMiddleware]
        sr_middleware_list: list[AIOHTTPMiddleware]
        usm_middleware_list: list[AIOHTTPMiddleware]

        conn_opts_factory = ConnOptionsMutatorsFactory()
        sr_factory = self.get_sr_factory(
            settings=self._settings, conn_opts_factory=conn_opts_factory, connectors_settings=connectors_settings
        )

        # Auth middlewares
        if self._settings.APP_TYPE == AppType.CLOUD_PUBLIC:
            auth_mw_list = [
                public_api_key_middleware(api_key=self._settings.PUBLIC_API_KEY),  # type: ignore  # TODO: fix
                public_usm_workaround_middleware(
                    us_base_url=self._settings.US_BASE_URL,
                    crypto_keys_config=self._settings.CRYPTO_KEYS_CONFIG,
                    dataset_id_match_info_code='ds_id',
                    conn_id_match_info_code='conn_id',
                    us_public_token=self._settings.US_PUBLIC_API_TOKEN,  # type: ignore  # TODO: fix
                    us_master_token=self._settings.US_MASTER_TOKEN,
                    tenant_resolver=TenantResolverYC(),
                ),
            ]
        elif self._settings.APP_TYPE == AppType.CLOUD:
            yc_auth_settings = self._settings.YC_AUTH_SETTINGS
            assert yc_auth_settings
            yc_auth_service = YCAuthService(
                allowed_folder_ids=None,
                yc_token_header_mode=YcTokenHeaderMode.INTERNAL,
                authorization_mode=AuthorizationModeYandexCloud(
                    folder_permission_to_check=yc_auth_settings.YC_AUTHORIZE_PERMISSION,
                    organization_permission_to_check=yc_auth_settings.YC_AUTHORIZE_PERMISSION,
                ),
                enable_cookie_auth=False,
                access_service_cfg=make_default_yc_auth_service_config(
                    endpoint=yc_auth_settings.YC_AS_ENDPOINT,
                ),
            )
            auth_mw_list = [yc_auth_service.middleware]
        elif self._settings.APP_TYPE == AppType.INTRANET:
            auth_mw_list = [
                blackbox_auth_middleware(),
            ]
        elif self._settings.APP_TYPE == AppType.TESTS:
            auth_mw_list = [
                auth_trust_middleware(
                    fake_user_id='_the_tests_asyncapp_user_id_',
                    fake_user_name='_the_tests_asyncapp_user_name_',
                )
            ]
        elif self._settings.APP_TYPE == AppType.DATA_CLOUD:
            yc_auth_settings = self._settings.YC_AUTH_SETTINGS
            assert yc_auth_settings
            dc_yc_auth_service = YCAuthService(
                allowed_folder_ids=None,
                yc_token_header_mode=YcTokenHeaderMode.INTERNAL,
                authorization_mode=AuthorizationModeDataCloud(
                    project_permission_to_check=yc_auth_settings.YC_AUTHORIZE_PERMISSION,
                ),
                enable_cookie_auth=False,
                access_service_cfg=make_default_yc_auth_service_config(
                    endpoint=yc_auth_settings.YC_AS_ENDPOINT,
                ),
            )
            auth_mw_list = [dc_yc_auth_service.middleware]
        elif self._settings.APP_TYPE == AppType.NEBIUS:
            yc_auth_settings = self._settings.YC_AUTH_SETTINGS
            assert yc_auth_settings
            yc_auth_service = YCAuthService(
                allowed_folder_ids=None,
                yc_token_header_mode=YcTokenHeaderMode.INTERNAL,
                authorization_mode=AuthorizationModeYandexCloud(
                    folder_permission_to_check=yc_auth_settings.YC_AUTHORIZE_PERMISSION,
                    organization_permission_to_check=yc_auth_settings.YC_AUTHORIZE_PERMISSION,
                ),
                enable_cookie_auth=False,
                access_service_cfg=make_default_yc_auth_service_config(
                    endpoint=yc_auth_settings.YC_AS_ENDPOINT,
                ),
            )
            auth_mw_list = [yc_auth_service.middleware]
        elif self._settings.APP_TYPE == AppType.DATA_CLOUD_EMBED:
            yc_auth_settings = self._settings.YC_AUTH_SETTINGS
            assert yc_auth_settings
            dc_yc_embed_auth_service = YCEmbedAuthService(
                authorization_mode=AuthorizationModeDataCloud(
                    project_permission_to_check=yc_auth_settings.YC_AUTHORIZE_PERMISSION,
                ),
            )
            auth_mw_list = [dc_yc_embed_auth_service.middleware]
        elif self._settings.APP_TYPE == AppType.CLOUD_EMBED:
            yc_auth_settings = self._settings.YC_AUTH_SETTINGS
            assert yc_auth_settings
            yc_embed_auth_service = YCEmbedAuthService(
                authorization_mode=AuthorizationModeYandexCloud(
                    folder_permission_to_check=yc_auth_settings.YC_AUTHORIZE_PERMISSION,
                    organization_permission_to_check=yc_auth_settings.YC_AUTHORIZE_PERMISSION,
                ),
            )
            auth_mw_list = [yc_embed_auth_service.middleware]
        else:
            raise ValueError(f"Unsupported auth mode: {self._settings.APP_TYPE}")

        # SR middlewares

        def ydb_is_cloud_mutator(
                conn_opts: ConnectOptions, conn: ExecutorBasedMixin
        ) -> Optional[ConnectOptions]:
            if isinstance(conn_opts, YDBConnectOptions):
                return conn_opts.clone(is_cloud=True)
            return None

        def ignore_managed_conn_opts_mutator(
                conn_opts: ConnectOptions, conn: ExecutorBasedMixin
        ) -> Optional[ConnectOptions]:
            if self._settings.MDB_FORCE_IGNORE_MANAGED_NETWORK:
                return conn_opts.clone(use_managed_network=False)
            return None

        if self._settings.APP_TYPE in (AppType.CLOUD, AppType.CLOUD_EMBED, AppType.NEBIUS):

            conn_opts_factory.add_mutator(ignore_managed_conn_opts_mutator)
            conn_opts_factory.add_mutator(ydb_is_cloud_mutator)

            sr_middleware_list = [
                services_registry_middleware(
                    services_registry_factory=sr_factory,
                    use_query_cache=self._settings.CACHES_ON,
                    use_mutation_cache=self._settings.MUTATIONS_CACHES_ON,
                    mutation_cache_default_ttl=self._settings.MUTATIONS_CACHES_DEFAULT_TTL,
                ),
            ]
        elif self._settings.APP_TYPE == AppType.CLOUD_PUBLIC:

            def public_timeout_conn_opts_mutator(
                    conn_opts: ConnectOptions, conn: ExecutorBasedMixin
            ) -> Optional[ConnectOptions]:
                if self._settings.PUBLIC_CH_QUERY_TIMEOUT is not None:
                    if isinstance(conn, ConnectionClickhouse):
                        return conn_opts.clone(
                            total_timeout=self._settings.PUBLIC_CH_QUERY_TIMEOUT,
                            max_execution_time=self._settings.PUBLIC_CH_QUERY_TIMEOUT - 2,
                        )
                return None

            conn_opts_factory.add_mutator(public_timeout_conn_opts_mutator)
            conn_opts_factory.add_mutator(ignore_managed_conn_opts_mutator)
            conn_opts_factory.add_mutator(ydb_is_cloud_mutator)

            sr_middleware_list = [
                services_registry_middleware(
                    services_registry_factory=sr_factory,
                    use_query_cache=self._settings.CACHES_ON,
                    use_mutation_cache=self._settings.MUTATIONS_CACHES_ON,
                    mutation_cache_default_ttl=self._settings.MUTATIONS_CACHES_DEFAULT_TTL,
                ),
            ]
        elif self._settings.APP_TYPE == AppType.INTRANET:
            def chyt_mirroring_conn_opts_mutator(
                    conn_opts: ConnectOptions, conn: ExecutorBasedMixin
            ) -> Optional[ConnectOptions]:
                if not isinstance(conn, BaseConnectionCHYTInternal):
                    return None
                mirroring_config = self._settings.CHYT_MIRRORING

                if mirroring_config is None:
                    return None
                conn_dto = conn.get_conn_dto()
                cluster_name = conn_dto.cluster.lower()  # type: ignore  # TODO: fix
                mirroring_clique_alias = (
                    mirroring_config.MAP.get(
                        (cluster_name, conn_dto.clique_alias)  # type: ignore  # TODO: fix
                    ) or mirroring_config.MAP.get((cluster_name, None))
                )
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
        elif self._settings.APP_TYPE in (AppType.DATA_CLOUD, AppType.DATA_CLOUD_EMBED):
            sr_middleware_list = [
                services_registry_middleware(
                    services_registry_factory=sr_factory,
                    use_query_cache=self._settings.CACHES_ON,
                    use_mutation_cache=self._settings.MUTATIONS_CACHES_ON,
                    mutation_cache_default_ttl=self._settings.MUTATIONS_CACHES_DEFAULT_TTL,
                ),
            ]
        elif self._settings.APP_TYPE == AppType.TESTS:
            sr_middleware_list = [
                services_registry_middleware(
                    services_registry_factory=sr_factory,
                    use_query_cache=self._settings.CACHES_ON,
                    use_mutation_cache=self._settings.MUTATIONS_CACHES_ON,
                    mutation_cache_default_ttl=self._settings.MUTATIONS_CACHES_DEFAULT_TTL,
                ),
            ]
        else:
            raise ValueError(f"Unsupported auth mode: {self._settings.APP_TYPE}")

        # US manager middleware list
        common_us_kw = dict(
            us_base_url=self._settings.US_BASE_URL,
            crypto_keys_config=self._settings.CRYPTO_KEYS_CONFIG,
        )
        if self._settings.APP_TYPE == AppType.CLOUD_PUBLIC:
            usm_middleware_list = [
                public_us_manager_middleware(
                    us_public_token=self._settings.US_PUBLIC_API_TOKEN, **common_us_kw  # type: ignore  # TODO: fix
                ),
            ]

        elif self._settings.APP_TYPE == AppType.TESTS:
            usm_middleware_list = [
                service_us_manager_middleware(us_master_token=self._settings.US_MASTER_TOKEN, **common_us_kw),  # type: ignore  # TODO: fix
                service_us_manager_middleware(us_master_token=self._settings.US_MASTER_TOKEN, as_user_usm=True, **common_us_kw),  # type: ignore  # TODO: fix
            ]

        elif self._settings.APP_TYPE in (AppType.DATA_CLOUD_EMBED, AppType.CLOUD_EMBED):
            usm_middleware_list = [
                us_manager_middleware(embed=True, **common_us_kw),  # type: ignore  # TODO: fix
                service_us_manager_middleware(us_master_token=self._settings.US_MASTER_TOKEN, **common_us_kw)  # type: ignore  # TODO: fix
            ]

        else:
            usm_middleware_list = [
                us_manager_middleware(**common_us_kw),  # type: ignore  # TODO: fix
                service_us_manager_middleware(us_master_token=self._settings.US_MASTER_TOKEN, **common_us_kw)  # type: ignore  # TODO: fix
            ]

        result = EnvSetupResult(
            auth_mw_list=auth_mw_list,
            sr_middleware_list=sr_middleware_list,
            usm_middleware_list=usm_middleware_list,
        )

        return result
