from __future__ import annotations

import abc
import contextlib
from typing import (
    Any,
    AsyncGenerator,
    Callable,
    ClassVar,
    NamedTuple,
    Optional,
)

import pytest
from redis.asyncio import Redis

from dl_api_commons.base_models import RequestContextInfo
from dl_api_commons.reporting.registry import DefaultReportingRegistry
from dl_compeng_pg.compeng_pg_base.data_processor_service_pg import CompEngPgConfig
from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_configs.crypto_keys import CryptoKeysConfig
from dl_configs.rqe import RQEConfig
from dl_configs.settings_submodels import RedisSettings
from dl_constants.enums import (
    ConnectionType,
    ProcessorType,
)
from dl_core.aio.web_app_services.data_processing.data_processor import DataProcessorService
from dl_core.aio.web_app_services.data_processing.factory import make_compeng_service
from dl_core.connections_security.base import InsecureConnectionSecurityManager
from dl_core.services_registry.conn_executor_factory import DefaultConnExecutorFactory
from dl_core.services_registry.entity_checker import EntityUsageChecker
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
from dl_core_testing.configuration import CoreTestEnvironmentConfiguration
from dl_core_testing.database import (
    CoreDbConfig,
    CoreReInitableDbDispenser,
    Db,
)
from dl_core_testing.fixtures.dispenser import DbCsvTableDispenser
from dl_db_testing.database.engine_wrapper import DbEngineConfig
from dl_testing.utils import get_root_certificates
from dl_utils.aio import ContextVarExecutor


class USConfig(NamedTuple):
    us_base_url: str
    us_auth_context: USAuthContextMaster
    us_crypto_keys_config: CryptoKeysConfig


class ServiceFixtureTextClass(metaclass=abc.ABCMeta):
    conn_type: ClassVar[ConnectionType]
    core_test_config: ClassVar[CoreTestEnvironmentConfiguration]
    connection_settings: ClassVar[Optional[ConnectorSettingsBase]] = None
    inst_specific_sr_factory: ClassVar[Optional[InstallationSpecificServiceRegistryFactory]] = None
    data_caches_enabled: ClassVar[bool] = False
    compeng_enabled: ClassVar[bool] = False

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
    def root_certificates(self) -> bytes:
        return get_root_certificates()

    @pytest.fixture(scope="session")
    def conn_exec_factory_async_env(self) -> bool:
        return False

    @contextlib.asynccontextmanager
    async def _make_redis(self, redis_settings: RedisSettings) -> AsyncGenerator[Redis, None]:
        redis_client: Redis = Redis(
            host=redis_settings.HOSTS[0],
            port=redis_settings.PORT,
            db=redis_settings.DB,
            password=redis_settings.PASSWORD,
        )
        try:
            yield redis_client
        finally:
            await redis_client.close()
            await redis_client.connection_pool.disconnect()

    @pytest.fixture(scope="function")
    async def caches_redis_client_factory(self) -> AsyncGenerator[Optional[Callable[[bool], Redis]], None]:
        if not self.data_caches_enabled:
            yield None

        else:
            redis_settings = self.core_test_config.get_redis_setting_maker().get_redis_settings_cache()
            async with self._make_redis(redis_settings=redis_settings) as redis_client:
                try:
                    yield lambda *args: redis_client
                finally:
                    await redis_client.close()
                    await redis_client.connection_pool.disconnect()

    @pytest.fixture(scope="function")
    async def data_processor_service_factory(
        self,
    ) -> AsyncGenerator[Optional[Callable[[ProcessorType], DataProcessorService]], None]:
        """
        PG CompEng pool fixture.

        Can only work properly in `db` tests, but should probably be okay to
        instantiate without it.
        """

        if not self.compeng_enabled:
            yield None

        else:
            compeng_type = ProcessorType.ASYNCPG
            processor = make_compeng_service(
                processor_type=compeng_type,
                config=CompEngPgConfig(url=self.core_test_config.get_compeng_url()),
            )
            await processor.initialize()
            try:
                yield (lambda *args: processor)
            finally:
                await processor.finalize()

    def service_registry_factory(
        self,
        conn_exec_factory_async_env: bool,
        conn_bi_context: RequestContextInfo,
        root_certificates_data: bytes,
        caches_redis_client_factory: Optional[Callable[[bool], Optional[Redis]]] = None,
        data_processor_service_factory: Optional[Callable[[bool], DataProcessorService]] = None,
        entity_usage_checker: Optional[EntityUsageChecker] = None,
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
                ca_data=root_certificates_data,
                entity_usage_checker=entity_usage_checker,
            ),
            connectors_settings={self.conn_type: self.connection_settings} if self.connection_settings else {},
            inst_specific_sr=(
                self.inst_specific_sr_factory.get_inst_specific_sr(sr_future_ref)
                if self.inst_specific_sr_factory is not None
                else None
            ),
            caches_redis_client_factory=caches_redis_client_factory,
            data_processor_service_factory=data_processor_service_factory,  # type: ignore  # 2024-01-29 # TODO: Argument "data_processor_service_factory" to "DefaultServicesRegistry" has incompatible type "Callable[[bool], DataProcessorService] | None"; expected "Callable[[ProcessorType], DataProcessorService] | None"  [arg-type]
            **kwargs,
        )
        sr_future_ref.fulfill(service_registry)
        return service_registry

    @pytest.fixture(scope="session")
    def conn_sync_service_registry(
        self,
        root_certificates: bytes,
        conn_bi_context: RequestContextInfo,
    ) -> ServicesRegistry:
        return self.service_registry_factory(
            conn_exec_factory_async_env=False,
            conn_bi_context=conn_bi_context,
            caches_redis_client_factory=None,
            root_certificates_data=root_certificates,
        )

    @pytest.fixture(scope="session")
    def conn_async_service_registry(
        self,
        root_certificates: bytes,
        conn_bi_context: RequestContextInfo,
        # caches_redis_client_factory: Optional[Callable[[bool], Redis]],  # FIXME: switch to function scope to use
    ) -> ServicesRegistry:
        return self.service_registry_factory(
            conn_exec_factory_async_env=True,
            conn_bi_context=conn_bi_context,
            root_certificates_data=root_certificates,
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
        root_certificates: bytes,
    ) -> AsyncUSManager:
        us_manager = AsyncUSManager(
            bi_context=conn_bi_context,
            services_registry=conn_async_service_registry,
            us_base_url=conn_us_config.us_base_url,
            us_auth_context=conn_us_config.us_auth_context,
            crypto_keys_config=conn_us_config.us_crypto_keys_config,
            ca_data=root_certificates,
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
        return

    @pytest.fixture(scope="class")
    def db(self, db_config: CoreDbConfig) -> Db:
        return self.db_dispenser.get_database(db_config)

    @pytest.fixture(scope="class", autouse=True)
    def db_is_ready(self, db: Db) -> None:
        return  # Just making sure that the database is available
