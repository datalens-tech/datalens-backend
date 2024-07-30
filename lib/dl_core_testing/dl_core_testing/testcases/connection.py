from __future__ import annotations

import abc
from typing import (
    TYPE_CHECKING,
    Callable,
    ClassVar,
    Generic,
    Optional,
    TypeVar,
    cast,
)

import pytest

from dl_constants.enums import RawSQLLevel
from dl_core.us_connection_base import (
    ConnectionBase,
    DataSourceTemplate,
)
from dl_core.us_manager.us_manager_async import AsyncUSManager
from dl_core.us_manager.us_manager_sync import SyncUSManager
from dl_core_testing.connection import make_saved_connection
from dl_core_testing.database import (
    Db,
    DbTable,
)
from dl_core_testing.fixtures.primitives import FixtureTableSpec
from dl_core_testing.fixtures.sample_tables import TABLE_SPEC_SAMPLE_SUPERSTORE
from dl_core_testing.testcases.service_base import (
    DbServiceFixtureTextClass,
    ServiceFixtureTextClass,
)
from dl_testing.regulated_test import RegulatedTestCase


if TYPE_CHECKING:
    from dl_core.connection_executors.async_base import AsyncConnExecutorBase
    from dl_core.connection_executors.sync_base import SyncConnExecutorBase
    from dl_core.services_registry.top_level import ServicesRegistry


_CONN_TV = TypeVar("_CONN_TV", bound=ConnectionBase)


class BaseConnectionTestClass(
    ServiceFixtureTextClass, DbServiceFixtureTextClass, Generic[_CONN_TV], metaclass=abc.ABCMeta
):
    raw_sql_level: ClassVar[Optional[RawSQLLevel]] = None

    @pytest.fixture(scope="function")
    def sync_us_manager(self, conn_default_sync_us_manager: SyncUSManager) -> SyncUSManager:
        return conn_default_sync_us_manager

    @pytest.fixture(scope="function")
    def async_us_manager(self, conn_default_async_us_manager: AsyncUSManager) -> AsyncUSManager:
        return conn_default_async_us_manager

    @abc.abstractmethod
    @pytest.fixture(scope="function")
    def connection_creation_params(self) -> dict:
        raise NotImplementedError

    @pytest.fixture(scope="function")
    def saved_connection(self, sync_us_manager: SyncUSManager, connection_creation_params: dict) -> _CONN_TV:
        conn = make_saved_connection(
            sync_usm=sync_us_manager,
            conn_type=self.conn_type,
            data_dict=connection_creation_params,
        )
        return cast(_CONN_TV, conn)

    @pytest.fixture(scope="function")
    def sync_conn_executor_factory(
        self,
        saved_connection: _CONN_TV,
        conn_sync_service_registry: ServicesRegistry,
    ) -> Callable[[], SyncConnExecutorBase]:
        def factory() -> SyncConnExecutorBase:
            ce_factory = conn_sync_service_registry.get_conn_executor_factory()
            return ce_factory.get_sync_conn_executor(saved_connection)

        return factory

    @pytest.fixture(scope="function")
    def async_conn_executor_factory(
        self,
        saved_connection: _CONN_TV,
        conn_async_service_registry: ServicesRegistry,
    ) -> Callable[[], AsyncConnExecutorBase]:
        def factory() -> AsyncConnExecutorBase:
            ce_factory = conn_async_service_registry.get_conn_executor_factory()
            return ce_factory.get_async_conn_executor(saved_connection)

        return factory

    @pytest.fixture(scope="class")
    def sample_table_spec(self) -> FixtureTableSpec:
        return TABLE_SPEC_SAMPLE_SUPERSTORE

    @pytest.fixture(scope="class")
    def sample_table(self, sample_table_spec: FixtureTableSpec, db: Db) -> DbTable:
        return self.db_table_dispenser.get_csv_table(db=db, spec=sample_table_spec)


class DefaultConnectionTestClass(RegulatedTestCase, BaseConnectionTestClass[_CONN_TV], Generic[_CONN_TV]):
    do_check_data_export_flag: ClassVar[bool] = False

    @abc.abstractmethod
    def check_saved_connection(self, conn: _CONN_TV, params: dict) -> None:
        raise NotImplementedError

    def test_make_connection(self, saved_connection: _CONN_TV, connection_creation_params: dict) -> None:
        conn = saved_connection
        self.check_saved_connection(conn=conn, params=connection_creation_params)

    def test_connection_test(
        self,
        saved_connection: _CONN_TV,
        sync_conn_executor_factory: Callable[[], SyncConnExecutorBase],
    ) -> None:
        def sync_conn_executor_factory_for_conn(connection: ConnectionBase) -> SyncConnExecutorBase:
            return sync_conn_executor_factory()

        conn = saved_connection
        # if test is unsuccessful, an exception is raised by the connection
        conn.test(conn_executor_factory=sync_conn_executor_factory_for_conn)

    @abc.abstractmethod
    def check_data_source_templates(self, conn: _CONN_TV, dsrc_templates: list[DataSourceTemplate]) -> None:
        # In case of disabled templates, explicitly assert `not dsrc_templates`
        raise NotImplementedError

    def test_connection_get_data_source_templates(
        self,
        saved_connection: _CONN_TV,
        sync_conn_executor_factory: Callable[[], SyncConnExecutorBase],
    ) -> None:
        def sync_conn_executor_factory_for_conn(connection: ConnectionBase) -> SyncConnExecutorBase:
            return sync_conn_executor_factory()

        conn = saved_connection
        dsrc_templates = conn.get_data_source_templates(conn_executor_factory=sync_conn_executor_factory_for_conn)

        self.check_data_source_templates(conn=conn, dsrc_templates=dsrc_templates)

    def test_connection_data_export_flag(
        self,
        saved_connection: ConnectionBase,
        sync_us_manager: SyncUSManager,
    ) -> None:
        if not self.do_check_data_export_flag:
            pytest.skip()

        conn = saved_connection
        us_manager = sync_us_manager
        assert conn.data_export_forbidden is False  # default value
        conn.data.data_export_forbidden = True
        assert conn.data_export_forbidden is True
        us_manager.save(conn)
        loaded_conn = us_manager.get_by_id(conn.uuid)
        assert loaded_conn.data.data_export_forbidden is True
        assert loaded_conn.data_export_forbidden is True
