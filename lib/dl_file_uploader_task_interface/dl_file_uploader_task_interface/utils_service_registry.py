from __future__ import annotations

import logging
import os
from typing import (
    TYPE_CHECKING,
    Optional,
)

import attr

from dl_api_commons.base_models import RequestContextInfo
from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_configs.crypto_keys import CryptoKeysConfig
from dl_configs.rqe import rqe_config_from_env
from dl_constants.enums import ConnectionType
from dl_core.retrier.policy import RetryPolicyFactory
from dl_core.services_registry.env_manager_factory import InsecureEnvManagerFactory
from dl_core.services_registry.sr_factories import DefaultSRFactory
from dl_core.services_registry.top_level import ServicesRegistry
from dl_core.united_storage_client import USAuthContextMaster
from dl_core.us_manager.us_manager_async import AsyncUSManager
from dl_file_uploader_worker_lib.settings import FileUploaderConnectorsSettings

from dl_connector_bundle_chs3.chs3_gsheets.core.constants import CONNECTION_TYPE_GSHEETS_V2
from dl_connector_bundle_chs3.chs3_yadocs.core.constants import CONNECTION_TYPE_YADOCS
from dl_connector_bundle_chs3.file.core.constants import CONNECTION_TYPE_FILE
from dl_connector_clickhouse.core.clickhouse_base.conn_options import CHConnectOptions


if TYPE_CHECKING:
    from dl_core.connection_models import ConnectOptions
    from dl_core.us_connection_base import ConnectionBase


LOGGER = logging.getLogger(__name__)


def create_sr_factory_from_env_vars(
    file_uploader_connectors_settings: FileUploaderConnectorsSettings,
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

    connectors_settings: dict[ConnectionType, ConnectorSettingsBase] = {}
    if file_uploader_connectors_settings.FILE is not None:
        connectors_settings = {
            CONNECTION_TYPE_FILE: file_uploader_connectors_settings.FILE,
            CONNECTION_TYPE_GSHEETS_V2: file_uploader_connectors_settings.FILE,
            CONNECTION_TYPE_YADOCS: file_uploader_connectors_settings.FILE,
        }
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
    us_master_token: str,
    ca_data: bytes,
    crypto_keys_config: CryptoKeysConfig,
    services_registry: ServicesRegistry,
    retry_policy_factory: RetryPolicyFactory,
    bi_context: Optional[RequestContextInfo] = None,
) -> AsyncUSManager:
    usm = AsyncUSManager(
        us_api_prefix="private",
        us_base_url=us_host,
        us_auth_context=USAuthContextMaster(us_master_token=us_master_token),
        crypto_keys_config=crypto_keys_config,
        bi_context=bi_context or RequestContextInfo.create_empty(),
        services_registry=services_registry,
        ca_data=ca_data,
        retry_policy_factory=retry_policy_factory,
    )

    return usm
