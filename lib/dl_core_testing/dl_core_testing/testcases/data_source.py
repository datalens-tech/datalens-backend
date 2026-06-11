from __future__ import annotations

import abc
from collections.abc import (
    Callable,
    Sequence,
)
from typing import (
    TYPE_CHECKING,
    ClassVar,
    TypeVar,
)
import uuid

import pytest
import shortuuid

from dl_constants.enums import UserDataType
from dl_core.connection_models import TableIdent
from dl_core.connectors.base.data_source_migration import (
    DataSourceMigrationInterface,
    get_data_source_migrator,
)
from dl_core.connectors.sql_base.data_source_migration import (
    SQLSubselectDSMI,
    SQLTableDSMI,
)
from dl_core.data_source.base import DataSource
from dl_core.data_source.sql import (
    SubselectDataSource,
    TableSQLDataSourceMixin,
)
from dl_core.data_source_spec.base import DataSourceSpec
from dl_core.data_source_spec.sql import SubselectDataSourceSpec
from dl_core.db import SchemaColumn
from dl_core.us_connection_base import ConnectionBase
from dl_core.us_manager.us_manager_sync import SyncUSManager
from dl_core_testing.database import Db
from dl_core_testing.testcases.connection import BaseConnectionTestClass

if TYPE_CHECKING:
    from dl_core.connection_executors.sync_base import SyncConnExecutorBase


SUBSELECT_DSRC_TV = TypeVar("SUBSELECT_DSRC_TV", bound=SubselectDataSource)


class BaseDataSourceTestClass[CONN_TV: ConnectionBase, DSRC_SPEC_TV: DataSourceSpec, DSRC_TV: DataSource](
    BaseConnectionTestClass[CONN_TV],
):
    DSRC_CLS: ClassVar[type[DSRC_TV]]

    @abc.abstractmethod
    @pytest.fixture(scope="class")
    def initial_data_source_spec(self) -> DSRC_SPEC_TV:
        raise NotImplementedError

    @pytest.fixture(scope="function")
    def data_source(
        self,
        sync_us_manager: SyncUSManager,
        saved_connection: CONN_TV,
        initial_data_source_spec: DSRC_SPEC_TV,
    ) -> DSRC_TV:
        return self.DSRC_CLS(
            id=uuid.uuid4().hex,
            us_entry_buffer=sync_us_manager.get_entry_buffer(),
            connection=saved_connection,
            spec=initial_data_source_spec,
            dataset_parameter_values={},
            dataset_template_enabled=False,
        )


class DefaultDataSourceTestClass[CONN_TV: ConnectionBase, DSRC_SPEC_TV: DataSourceSpec, DSRC_TV: DataSource](
    BaseDataSourceTestClass[CONN_TV, DSRC_SPEC_TV, DSRC_TV],
):
    def test_data_source_exists(
        self,
        data_source: DSRC_TV,
        sync_conn_executor_factory: Callable[[], SyncConnExecutorBase],
    ) -> None:
        dsrc = data_source
        result = dsrc.source_exists(conn_executor_factory=sync_conn_executor_factory)
        assert result

    @abc.abstractmethod
    def get_expected_simplified_schema(self) -> list[tuple[str, UserDataType]]:
        raise NotImplementedError

    def test_get_raw_schema(
        self,
        data_source: DSRC_TV,
        sync_conn_executor_factory: Callable[[], SyncConnExecutorBase],
    ) -> None:
        raw_schema = data_source.get_schema_info(conn_executor_factory=sync_conn_executor_factory).schema
        simplified_schema = [(col.title, col.user_type) for col in raw_schema]
        expected_schema = self.get_expected_simplified_schema()
        print("schema:")
        for col in simplified_schema:
            print(col)

        # TODO: This is a workaround for YDB because raw schema is sorted different. Remove it when the issue is fixed.
        if data_source.conn_type.name == "ydb":
            simplified_schema = sorted(simplified_schema, key=lambda v: v[0])
            expected_schema = sorted(expected_schema, key=lambda v: v[0])

        assert simplified_schema == expected_schema

    def _check_migration_dtos(
        self,
        data_source: DSRC_TV,
        migration_dtos: Sequence[DataSourceMigrationInterface],
    ) -> None:
        pass

    def _check_migration_dtos_table(  # TODO: Plug it in
        self,
        data_source: DSRC_TV,
        migration_dtos: Sequence[DataSourceMigrationInterface],
    ) -> None:
        assert isinstance(data_source, TableSQLDataSourceMixin)
        assert migration_dtos[0] == SQLTableDSMI(  # type: ignore  # 2024-01-29 # TODO: Unexpected keyword argument "db_name" for "SQLTableDSMI"  [call-arg]
            db_name=getattr(data_source, "db_name", None),
            schema_name=getattr(data_source, "schema_name", None),
            table_name=data_source.table_name,
        )

    def _check_migration_dtos_subselect(  # TODO: Plug it in
        self,
        data_source: DSRC_TV,
        migration_dtos: Sequence[DataSourceMigrationInterface],
    ) -> None:
        assert isinstance(data_source, SubselectDataSource)
        assert migration_dtos[0] == SQLSubselectDSMI(
            subsql=data_source.subsql,
        )

    def test_export_dsrc_migration_dtos(
        self,
        data_source: DSRC_TV,
        sync_us_manager: SyncUSManager,
    ) -> None:
        migrator = get_data_source_migrator(self.conn_type)
        source_spec = data_source.spec
        migration_dtos = migrator.export_migration_dtos(data_source_spec=source_spec)
        self._check_migration_dtos(data_source=data_source, migration_dtos=migration_dtos)

    def test_get_cache_key_part(self, data_source: SUBSELECT_DSRC_TV) -> None:
        cache_key_part = data_source.get_cache_key_part()

        data_source_sql_key_part = None

        for key_part in cache_key_part.key_parts:
            if key_part.part_type == "data_source_sql":
                data_source_sql_key_part = key_part
                break

        assert data_source_sql_key_part is not None
        assert data_source_sql_key_part.part_content != ""


class DataSourceTestByViewClass[
    CONN_TV: ConnectionBase, SUBSELECT_DSRC_SPEC_TV: SubselectDataSourceSpec, SUBSELECT_DSRC_TV: SubselectDataSource
](
    BaseDataSourceTestClass[CONN_TV, SUBSELECT_DSRC_SPEC_TV, SUBSELECT_DSRC_TV],
):
    def postprocess_view_schema_column(self, schema_col: SchemaColumn) -> SchemaColumn:
        """
        Place for marking known inevitable discrepancies between subselect
        schema and view schema.
        """
        return schema_col

    def postprocess_view_schema(
        self, view_schema: list[SchemaColumn], cursor_schema: list[SchemaColumn]
    ) -> list[SchemaColumn]:
        return [self.postprocess_view_schema_column(schema_col) for schema_col in view_schema]

    @pytest.fixture(scope="function")
    def subselect_view(self, initial_data_source_spec: SUBSELECT_DSRC_SPEC_TV, db: Db) -> str:
        name = f"test_view_from_subselect_{shortuuid.uuid().lower()}"
        db.execute(f"create view {name} as select * from ({initial_data_source_spec.subsql}) source")
        return name

    def test_view_from_subselect(
        self,
        data_source: SUBSELECT_DSRC_TV,
        db: Db,
        sync_conn_executor_factory: Callable[[], SyncConnExecutorBase],
        subselect_view: str,
    ) -> None:
        """Compare the subselect schema to a view-based schema"""
        dsrc = data_source
        conn_executor = sync_conn_executor_factory()
        schema = dsrc.get_schema_info(conn_executor_factory=sync_conn_executor_factory).schema

        # A bit of a hack, for easier inter-db testing:
        view_schema_info = conn_executor.get_table_schema_info(
            TableIdent(db_name=None, schema_name=None, table_name=subselect_view),
        )
        view_schema = dsrc._postprocess_raw_schema_from_db(view_schema_info).schema
        view_schema = self.postprocess_view_schema(view_schema, cursor_schema=schema)

        # for a more readable diff:
        if len(schema) == len(view_schema):
            sc_mismatch = [sc for sc, vsc in zip(schema, view_schema, strict=True) if sc != vsc]
            vsc_mismatch = [vsc for sc, vsc in zip(schema, view_schema, strict=True) if sc != vsc]
            if sc_mismatch != vsc_mismatch:
                print(f"{schema=}\n{view_schema=}")
            assert sc_mismatch == vsc_mismatch

        assert schema == tuple(view_schema)


class SQLDataSourceTestClass[CONN_TV: ConnectionBase, DSRC_SPEC_TV: DataSourceSpec, DSRC_TV: DataSource](
    BaseDataSourceTestClass[CONN_TV, DSRC_SPEC_TV, DSRC_TV],
):
    QUERY_PATTERN: ClassVar[str]

    def test_data_source_query(self, data_source: DSRC_TV) -> None:
        dsrc = data_source
        sql_query = str(dsrc.get_sql_source())
        assert self.QUERY_PATTERN in sql_query
