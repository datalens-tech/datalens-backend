from __future__ import annotations

import logging
import os
from typing import (
    TYPE_CHECKING,
    Optional,
)

import attr

from dl_api_commons.base_models import RequestContextInfo
from dl_configs.crypto_keys import CryptoKeysConfig
from dl_configs.rqe import rqe_config_from_env
from dl_core.connectors.settings.base import ConnectorSettings
from dl_core.services_registry.env_manager_factory import InsecureEnvManagerFactory
from dl_core.services_registry.sr_factories import DefaultSRFactory
from dl_core.services_registry.top_level import ServicesRegistry
from dl_core.united_storage_client import USAuthContextPrivateBase
from dl_core.us_manager.us_manager_async import AsyncUSManager
import dl_retrier

from dl_connector_clickhouse.core.clickhouse_base.conn_options import CHConnectOptions


if TYPE_CHECKING:
    from dl_core.connection_models import ConnectOptions
    from dl_core.us_connection_base import ConnectionBase


LOGGER = logging.getLogger(__name__)


def create_sr_factory_from_env_vars(
    connectors_settings: dict[str, ConnectorSettings],
    ca_data: bytes,
) -> DefaultSRFactory:
    def get_conn_options(conn: ConnectionBase) -> Optional[ConnectOptions]:
        opts = conn.get_conn_options()

        if isinstance(opts, CHConnectOptions):
            opts = attr.evolve(opts, max_execution_time=int(os.environ.get("CH_MAX_EXECUTION_TIME", 1 * 60 * 60)))

        return attr.evolve(
            opts,
            rqe_total_timeout=int(os.environ.get("RQE_TOTAL_TIMEOUT", 1 * 60 * 60)),
            rqe_sock_read_timeout=int(os.environ.get("RQE_SOCK_READ_TIMEOUT", 30 * 60)),
        )

    return DefaultSRFactory(
        rqe_config=rqe_config_from_env(),
        async_env=True,
        connect_options_factory=get_conn_options,
        env_manager_factory=InsecureEnvManagerFactory(),
        connectors_settings=connectors_settings,
        ca_data=ca_data,
    )


def get_async_service_us_manager(
    us_host: str,
    us_auth_context: USAuthContextPrivateBase,
    ca_data: bytes,
    crypto_keys_config: CryptoKeysConfig,
    services_registry: ServicesRegistry,
    retry_policy_factory: dl_retrier.BaseRetryPolicyFactory,
    bi_context: Optional[RequestContextInfo] = None,
) -> AsyncUSManager:
    usm = AsyncUSManager(
        us_api_prefix="private",
        us_base_url=us_host,
        us_auth_context=us_auth_context,
        crypto_keys_config=crypto_keys_config,
        bi_context=bi_context or RequestContextInfo.create_empty(),
        services_registry=services_registry,
        ca_data=ca_data,
        retry_policy_factory=retry_policy_factory,
    )

    return usm
