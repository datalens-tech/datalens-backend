from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Optional

import attr

from bi_configs.rqe import rqe_config_from_env
from bi_configs.crypto_keys import CryptoKeysConfig
from bi_configs.connectors_settings import ConnectorsSettingsByType

from bi_core.connectors.clickhouse_base.conn_options import CHConnectOptions
from bi_core.connections_security.base import InsecureConnectionSecurityManager, ConnectionSecurityManager
from bi_core.services_registry.env_manager_factory import DefaultEnvManagerFactory
from bi_core.services_registry.sr_factories import DefaultSRFactory
from bi_api_commons.base_models import RequestContextInfo
from bi_core.united_storage_client import USAuthContextMaster
from bi_core.us_manager.us_manager_async import AsyncUSManager
from bi_core.services_registry.top_level import ServicesRegistry


if TYPE_CHECKING:
    from bi_core.connection_models import ConnectOptions
    from bi_core.us_connection_base import ExecutorBasedMixin


LOGGER = logging.getLogger(__name__)


class InsecureEnvManagerFactory(DefaultEnvManagerFactory):
    def make_security_manager(self, request_context_info: RequestContextInfo) -> ConnectionSecurityManager:
        return InsecureConnectionSecurityManager()


def create_sr_factory_from_env_vars(connectors_settings: ConnectorsSettingsByType) -> DefaultSRFactory:

    def get_conn_options(conn: ExecutorBasedMixin) -> Optional[ConnectOptions]:
        opts = conn.get_conn_options()

        if isinstance(opts, CHConnectOptions):
            opts = attr.evolve(
                opts,
                max_execution_time=int(os.environ.get('CH_MAX_EXECUTION_TIME', 1 * 60 * 60))
            )

        return attr.evolve(
            opts,
            rqe_total_timeout=int(os.environ.get('RQE_TOTAL_TIMEOUT', 1 * 60 * 60)),
            rqe_sock_read_timeout=int(os.environ.get('RQE_SOCK_READ_TIMEOUT', 30 * 60)),
        )

    return DefaultSRFactory(
        rqe_config=rqe_config_from_env(),
        async_env=True,
        connect_options_factory=get_conn_options,
        env_manager_factory=InsecureEnvManagerFactory(),
        connectors_settings=connectors_settings,
    )


def get_async_service_us_manager(
        us_host: str,
        us_master_token: str,
        crypto_keys_config: CryptoKeysConfig,
        bi_context: Optional[RequestContextInfo] = None,
        services_registry: Optional[ServicesRegistry] = None,
) -> AsyncUSManager:
    usm = AsyncUSManager(
        us_api_prefix='private',
        us_base_url=us_host,
        us_auth_context=USAuthContextMaster(us_master_token=us_master_token),
        crypto_keys_config=crypto_keys_config,
        bi_context=bi_context or RequestContextInfo.create_empty(),
        services_registry=services_registry,
    )

    return usm
