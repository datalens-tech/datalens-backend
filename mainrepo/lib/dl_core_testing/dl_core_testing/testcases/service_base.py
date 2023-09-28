from __future__ import annotations

import abc
from typing import (
    Any,
    ClassVar,
    NamedTuple,
    Optional,
)

import pytest

from dl_api_commons.base_models import RequestContextInfo
from dl_api_commons.reporting.registry import DefaultReportingRegistry
from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_configs.crypto_keys import CryptoKeysConfig
from dl_configs.rqe import RQEConfig
from dl_constants.enums import ConnectionType
from dl_core.connections_security.base import InsecureConnectionSecurityManager
from dl_core.services_registry.conn_executor_factory import DefaultConnExecutorFactory
from dl_core.services_registry.inst_specific_sr import InstallationSpecificServiceRegistryFactory
from dl_core.services_registry.top_level import (
    DefaultServicesRegistry,
    ServicesRegistry,
)
from dl_core.united_storage_client import USAuthContextMaster
from dl_core.us_manager.mutation_cache.usentry_mutation_cache_factory import DefaultUSEntryMutationCacheFactory
from dl_core.us_manager.us_manager_async import AsyncUSManager
from dl_core.us_manager.us_manager_sync import SyncUSManager
from dl_core.utils import FutureRef
from dl_core_testing.configuration import CoreTestEnvironmentConfigurationBase
from dl_core_testing.database import (
    CoreDbConfig,
    CoreReInitableDbDispenser,
    Db,
)
from dl_core_testing.fixtures.dispenser import DbCsvTableDispenser
from dl_db_testing.database.engine_wrapper import DbEngineConfig
from dl_utils.aio import ContextVarExecutor


class USConfig(NamedTuple):
    us_base_url: str
    us_auth_context: USAuthContextMaster
    us_crypto_keys_config: CryptoKeysConfig


class ServiceFixtureTextClass(metaclass=abc.ABCMeta):
    conn_type: ClassVar[ConnectionType]
    core_test_config: ClassVar[CoreTestEnvironmentConfigurationBase]
    connection_settings: ClassVar[Optional[ConnectorSettingsBase]] = None
    inst_specific_sr_factory: ClassVar[Optional[InstallationSpecificServiceRegistryFactory]] = None

    @pytest.fixture(scope="session")
    def conn_us_config(self) -> USConfig:
        us_env_config = self.core_test_config.get_us_config()
        return USConfig(
            us_base_url=us_env_config.us_host,
            us_auth_context=USAuthContextMaster(us_env_config.us_master_token),
            us_crypto_keys_config=self.core_test_config.get_crypto_keys_config(),
        )

    @pytest.fixture(scope="session")
    def conn_bi_context(self) -> RequestContextInfo:
        bi_context = RequestContextInfo.create_empty()
        return bi_context

    @pytest.fixture(scope="session")
    def conn_exec_factory_async_env(self) -> bool:
        return False

    def service_registry_factory(
        self,
        conn_exec_factory_async_env: bool,
        conn_bi_context: RequestContextInfo,
        **kwargs: Any,
    ) -> ServicesRegistry:
        sr_future_ref: FutureRef[ServicesRegistry] = FutureRef()
        service_registry = DefaultServicesRegistry(
            rci=conn_bi_context,
            mutations_cache_factory=DefaultUSEntryMutationCacheFactory(),
            reporting_registry=DefaultReportingRegistry(
                rci=conn_bi_context,
            ),
            conn_exec_factory=DefaultConnExecutorFactory(
                async_env=conn_exec_factory_async_env,
                force_non_rqe_mode=True,
                rqe_config=RQEConfig.get_default(),  # Not used because RQE is disabled
                services_registry_ref=sr_future_ref,
                conn_sec_mgr=InsecureConnectionSecurityManager(),
                tpe=ContextVarExecutor(),
            ),
            connectors_settings={self.conn_type: self.connection_settings} if self.connection_settings else {},
            inst_specific_sr=(
                self.inst_specific_sr_factory.get_inst_specific_sr(sr_future_ref)
                if self.inst_specific_sr_factory is not None
                else None
            ),
            **kwargs,
        )
        sr_future_ref.fulfill(service_registry)
        return service_registry

    @pytest.fixture(scope="session")
    def conn_sync_service_registry(
        self,
        conn_bi_context: RequestContextInfo,
    ) -> ServicesRegistry:
        return self.service_registry_factory(
            conn_exec_factory_async_env=False,
            conn_bi_context=conn_bi_context,
        )

    @pytest.fixture(scope="session")
    def conn_async_service_registry(
        self,
        conn_bi_context: RequestContextInfo,
    ) -> ServicesRegistry:
        return self.service_registry_factory(
            conn_exec_factory_async_env=True,
            conn_bi_context=conn_bi_context,
        )

    @pytest.fixture(scope="session")
    def conn_default_service_registry(
        self,
        conn_exec_factory_async_env: bool,
        conn_sync_service_registry: ServicesRegistry,
        conn_async_service_registry: ServicesRegistry,
    ) -> ServicesRegistry:
        if conn_exec_factory_async_env:
            return conn_async_service_registry
        return conn_sync_service_registry

    @pytest.fixture(scope="class")
    def conn_default_sync_us_manager(
        self,
        conn_us_config: USConfig,
        conn_bi_context: RequestContextInfo,
        conn_default_service_registry: ServicesRegistry,
    ) -> SyncUSManager:
        us_manager = SyncUSManager(
            bi_context=conn_bi_context,
            services_registry=conn_default_service_registry,
            us_base_url=conn_us_config.us_base_url,
            us_auth_context=conn_us_config.us_auth_context,
            crypto_keys_config=conn_us_config.us_crypto_keys_config,
        )
        return us_manager

    @pytest.fixture(scope="class")
    def conn_default_async_us_manager(
        self,
        conn_us_config: USConfig,
        conn_bi_context: RequestContextInfo,
        conn_async_service_registry: ServicesRegistry,
    ) -> AsyncUSManager:
        us_manager = AsyncUSManager(
            bi_context=conn_bi_context,
            services_registry=conn_async_service_registry,
            us_base_url=conn_us_config.us_base_url,
            us_auth_context=conn_us_config.us_auth_context,
            crypto_keys_config=conn_us_config.us_crypto_keys_config,
        )
        return us_manager


class DbServiceFixtureTextClass(metaclass=abc.ABCMeta):
    conn_type: ClassVar[ConnectionType]  # FIXME: Remove after conn_type is removed from Db
    db_dispenser: ClassVar[CoreReInitableDbDispenser] = CoreReInitableDbDispenser()
    db_table_dispenser = DbCsvTableDispenser()

    @abc.abstractmethod
    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        pass

    @pytest.fixture(scope="class")
    def engine_params(self) -> dict:
        return {}

    @pytest.fixture(scope="class")
    def engine_config(self, db_url: str, engine_params: dict) -> DbEngineConfig:
        return DbEngineConfig(url=db_url, engine_params=engine_params)

    @pytest.fixture(scope="class")
    def db_config(self, engine_config: DbEngineConfig) -> CoreDbConfig:
        return CoreDbConfig(
            engine_config=engine_config,
            conn_type=self.conn_type,
        )

    @pytest.fixture(scope="class", autouse=True)
    def _add_db_init_hook(self, db_config: CoreDbConfig) -> None:
        self.db_dispenser.add_reinit_hook(
            # This method is idempotent, so it's OK that this will get called for every subclass
            db_config=db_config,
            reinit_hook=self.db_reinit_hook,
        )

    def db_reinit_hook(self) -> None:
        pass

    @pytest.fixture(scope="class")
    def db(self, db_config: CoreDbConfig) -> Db:
        return self.db_dispenser.get_database(db_config)

    @pytest.fixture(scope="class", autouse=True)
    def db_is_ready(self, db: Db) -> str:
        pass  # Just making sure that the database is available
