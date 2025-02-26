from __future__ import annotations

import asyncio
import contextlib
import functools
from typing import (
    TYPE_CHECKING,
    AsyncGenerator,
    Callable,
    ClassVar,
    Generator,
    Generic,
    Optional,
    Sequence,
    TypeVar,
)

import attr
import pytest
import pytest_asyncio
import shortuuid
import sqlalchemy as sa
from sqlalchemy.types import TypeEngine

from dl_constants.enums import (
    ConnectionType,
    UserDataType,
)
from dl_core.connection_executors.common_base import ConnExecutorQuery
from dl_core.connection_models.common_models import (
    DBIdent,
    SATextTableDefinition,
    SchemaIdent,
    TableIdent,
)
import dl_core.exc as core_exc
from dl_core.us_connection_base import ConnectionBase
from dl_core_testing.database import (
    C,
    Db,
    DbTable,
    make_table,
)
from dl_core_testing.testcases.connection import BaseConnectionTestClass
from dl_testing.regulated_test import RegulatedTestCase
from dl_type_transformer.native_type import (
    CommonNativeType,
    GenericNativeType,
    norm_native_type,
)


if TYPE_CHECKING:
    from dl_core.connection_executors.async_base import AsyncConnExecutorBase
    from dl_core.connection_executors.sync_base import SyncConnExecutorBase


_CONN_TV = TypeVar("_CONN_TV", bound=ConnectionBase)


class BaseConnectionExecutorTestClass(RegulatedTestCase, BaseConnectionTestClass[_CONN_TV], Generic[_CONN_TV]):
    @pytest.fixture(scope="function")
    def sync_connection_executor(
        self,
        sync_conn_executor_factory: Callable[[], SyncConnExecutorBase],
    ) -> Generator[SyncConnExecutorBase, None, None]:
        sync_conn_executor = sync_conn_executor_factory()
        yield sync_conn_executor_factory()
        sync_conn_executor.close()

    @pytest_asyncio.fixture(scope="function")
    async def async_connection_executor(
        self,
        async_conn_executor_factory: Callable[[], AsyncConnExecutorBase],
    ) -> AsyncGenerator[AsyncConnExecutorBase, None]:
        async_conn_executor = async_conn_executor_factory()
        yield async_conn_executor_factory()
        await async_conn_executor.close()


class DefaultSyncAsyncConnectionExecutorCheckBase(BaseConnectionExecutorTestClass[_CONN_TV], Generic[_CONN_TV]):
    @pytest.fixture(scope="function")
    def db_ident(self) -> DBIdent:
        raise NotImplementedError

    @pytest.fixture(scope="function")
    def existing_table_ident(self, sample_table: DbTable) -> TableIdent:
        # Using sample table by default, but this can be overridden by redefining this fixture
        return TableIdent(
            db_name=sample_table.db.name,
            schema_name=sample_table.schema,
            table_name=sample_table.name,
        )

    @pytest.fixture(scope="function")
    def nonexistent_table_ident(self, existing_table_ident: TableIdent) -> TableIdent:
        return existing_table_ident.clone(table_name=f"nonexistent_table_{shortuuid.uuid()}")

    def check_db_version(self, db_version: Optional[str]) -> None:
        pass

    @pytest.fixture(scope="class")
    def query_for_session_check(self) -> str:
        return "SELECT 567"

    @contextlib.contextmanager
    def check_closing_sql_sessions(self, db: Db) -> Generator[None, None, None]:
        old_cnt = db.count_sql_sessions()
        yield
        new_cnt = db.count_sql_sessions()
        assert old_cnt == new_cnt, f"Expected {old_cnt} sessions, got {new_cnt}"


class DefaultSyncConnectionExecutorTestSuite(DefaultSyncAsyncConnectionExecutorCheckBase[_CONN_TV], Generic[_CONN_TV]):
    @pytest.fixture(scope="session")
    def conn_exec_factory_async_env(self) -> bool:
        return False

    def test_get_db_version(self, sync_connection_executor: SyncConnExecutorBase, db_ident: DBIdent) -> None:
        db_version = sync_connection_executor.get_db_version(db_ident)
        self.check_db_version(db_version)

    def test_test(self, sync_connection_executor: SyncConnExecutorBase) -> None:
        sync_connection_executor.test()

    def test_get_table_names(
        self,
        sample_table: DbTable,
        sync_connection_executor: SyncConnExecutorBase,
    ) -> None:
        # at the moment, checks that sample table is listed among the others

        tables = [sample_table]
        expected_table_names = set(table.name for table in tables)

        actual_tables = sync_connection_executor.get_tables(
            SchemaIdent(
                db_name=sample_table.db.name,
                schema_name=sample_table.schema,
            )
        )
        actual_table_names = [tid.table_name for tid in actual_tables]

        assert set(actual_table_names).issuperset(expected_table_names)

    def test_table_exists(
        self,
        sync_connection_executor: SyncConnExecutorBase,
        existing_table_ident: TableIdent,
    ) -> None:
        assert sync_connection_executor.is_table_exists(existing_table_ident)

    def test_table_not_exists(
        self,
        sync_connection_executor: SyncConnExecutorBase,
        nonexistent_table_ident: TableIdent,
    ) -> None:
        assert not sync_connection_executor.is_table_exists(nonexistent_table_ident)

    @attr.s(frozen=True)
    class CD:
        sa_type: TypeEngine = attr.ib()
        # Expected data
        user_type: UserDataType = attr.ib()
        nullable: bool = attr.ib(default=True)
        nt_name: Optional[str] = attr.ib(default=None)
        nt: Optional[GenericNativeType] = attr.ib(default=None)

        def get_expected_native_type(self, conn_type: ConnectionType) -> GenericNativeType:
            if self.nt is not None:
                return self.nt
            name = norm_native_type(self.nt_name if self.nt_name is not None else self.sa_type)
            assert name is not None
            return CommonNativeType(name=name, nullable=self.nullable)

    def get_schemas_for_type_recognition(self) -> dict[str, Sequence[CD]]:
        return {
            "standard_types": [
                self.CD(sa.Integer(), UserDataType.integer),
                self.CD(sa.Float(), UserDataType.float),
                self.CD(sa.String(length=256), UserDataType.string),
                self.CD(sa.Date(), UserDataType.date),
                self.CD(sa.DateTime(), UserDataType.genericdatetime),
            ],
        }

    def test_type_recognition(
        self, request: pytest.FixtureRequest, db: Db, sync_connection_executor: SyncConnExecutorBase
    ) -> None:
        for schema_name, type_schema in self.get_schemas_for_type_recognition().items():
            columns = [
                sa.Column(name=f"c_{shortuuid.uuid().lower()}", type_=column_data.sa_type)
                for column_data in type_schema
            ]
            sa_table = db.table_from_columns(columns=columns)

            db.create_table(sa_table)
            request.addfinalizer(functools.partial(db.drop_table, sa_table))

            detected_columns = sync_connection_executor.get_table_schema_info(
                table_def=TableIdent(db_name=db.name, schema_name=sa_table.schema, table_name=sa_table.name)
            ).schema
            assert len(detected_columns) == len(type_schema), f"Incorrect number of columns in schema {schema_name}"
            for col_idx, (expected_col, detected_col) in enumerate(zip(type_schema, detected_columns, strict=True)):
                assert detected_col.user_type == expected_col.user_type, (
                    f"Incorrect user type detected for schema {schema_name} col #{col_idx}: "
                    f"expected {expected_col.user_type.name}, got {detected_col.user_type.name}"
                )
                expected_native_type = expected_col.get_expected_native_type(self.conn_type)
                assert (
                    detected_col.native_type == expected_native_type
                ), f"Incorrect native type detected for schema {schema_name} col #{col_idx}: expected {repr(expected_native_type)}, got {repr(detected_col.native_type)}"

    def test_simple_select(self, sync_connection_executor: SyncConnExecutorBase) -> None:
        query = ConnExecutorQuery(query=sa.select([sa.literal(1)]))
        result = next(iter(sync_connection_executor.execute(query).result))
        assert len(result) == 1
        assert result[0] == (1,)

    def test_error_on_select_from_nonexistent_source(
        self,
        db: Db,
        sync_connection_executor: SyncConnExecutorBase,
        nonexistent_table_ident: TableIdent,
    ) -> None:
        query = ConnExecutorQuery(query=f"SELECT * from {db.quote(nonexistent_table_ident.table_name)}")
        with pytest.raises(core_exc.SourceDoesNotExist):
            sync_connection_executor.execute(query)

    def test_closing_sql_sessions(
        self, db: Db, sync_connection_executor: SyncConnExecutorBase, query_for_session_check: str
    ) -> None:
        with self.check_closing_sql_sessions(db=db):
            for _i in range(5):
                sync_connection_executor.execute(ConnExecutorQuery(query=query_for_session_check))

    subselect_query_for_schema_test: ClassVar[str] = "(SELECT 1 AS num) AS source"

    @pytest.mark.parametrize("case", ["table", "subselect"])
    def test_get_table_schema_info(
        self,
        case: str,
        sync_connection_executor: SyncConnExecutorBase,
        existing_table_ident: TableIdent,
    ) -> None:
        # Just tests that the adapter can successfully retrieve the schema.
        # Data source tests check this in more detail
        table_def = {
            "table": existing_table_ident,
            "subselect": SATextTableDefinition(
                text=sa.sql.elements.TextClause(self.subselect_query_for_schema_test),
            ),
        }[case]
        detected_columns = sync_connection_executor.get_table_schema_info(table_def=table_def).schema
        assert len(detected_columns) > 0

    def test_get_table_schema_info_for_nonexistent_table(
        self,
        sync_connection_executor: SyncConnExecutorBase,
        nonexistent_table_ident: TableIdent,
    ) -> None:
        with pytest.raises(core_exc.DLBaseException):
            sync_connection_executor.get_table_schema_info(table_def=nonexistent_table_ident)


class DefaultAsyncConnectionExecutorTestSuite(DefaultSyncAsyncConnectionExecutorCheckBase[_CONN_TV], Generic[_CONN_TV]):
    @pytest.fixture(scope="session")
    def conn_exec_factory_async_env(self) -> bool:
        return True

    async def test_get_db_version(self, async_connection_executor: AsyncConnExecutorBase, db_ident: DBIdent) -> None:
        db_version = await async_connection_executor.get_db_version(db_ident)
        self.check_db_version(db_version)

    async def test_test(self, async_connection_executor: AsyncConnExecutorBase) -> None:
        await async_connection_executor.test()

    async def test_multiple_connection_test(self, async_connection_executor: AsyncConnExecutorBase) -> None:
        coros = (async_connection_executor.test() for _ in range(10))
        await asyncio.gather(*coros)

    async def test_table_exists(
        self,
        async_connection_executor: AsyncConnExecutorBase,
        existing_table_ident: TableIdent,
    ) -> None:
        assert await async_connection_executor.is_table_exists(existing_table_ident)

    async def test_table_not_exists(
        self,
        async_connection_executor: AsyncConnExecutorBase,
        nonexistent_table_ident: TableIdent,
    ) -> None:
        assert not await async_connection_executor.is_table_exists(nonexistent_table_ident)

    async def test_simple_select(self, async_connection_executor: AsyncConnExecutorBase) -> None:
        query = ConnExecutorQuery(query=sa.select([sa.literal(1)]))
        result = await anext(aiter((await async_connection_executor.execute(query)).result))
        assert len(result) == 1
        assert result[0] == (1,)

    async def test_select_data(self, sample_table: DbTable, async_connection_executor: AsyncConnExecutorBase) -> None:
        n_rows = 3
        result = await async_connection_executor.execute(
            ConnExecutorQuery(
                query=sa.select(columns=sample_table.table.columns)
                .select_from(sample_table.table)
                .order_by(sample_table.table.columns[0])
                .limit(n_rows),
                chunk_size=6,
            )
        )
        rows = await result.get_all()
        assert len(rows) == n_rows

    async def test_cast_row_to_output(
        self,
        sample_table: DbTable,
        async_connection_executor: AsyncConnExecutorBase,
    ) -> None:
        result = await async_connection_executor.execute(
            ConnExecutorQuery(
                sa.select(columns=[sa.literal(1), sa.literal(2), sa.literal(3)])
                .select_from(sample_table.table)
                .limit(1),
                user_types=[
                    UserDataType.boolean,
                    UserDataType.float,
                    UserDataType.integer,
                ],
            )
        )
        rows = await result.get_all()
        assert rows == [(True, 2.0, 3)], rows

    async def test_error_on_select_from_nonexistent_source(
        self,
        db: Db,
        async_connection_executor: AsyncConnExecutorBase,
        nonexistent_table_ident: TableIdent,
    ) -> None:
        query = ConnExecutorQuery(query=f"SELECT * from {db.quote(nonexistent_table_ident.table_name)}")
        with pytest.raises(core_exc.SourceDoesNotExist):
            await async_connection_executor.execute(query)

    async def test_closing_sql_sessions(
        self, db: Db, async_connection_executor: AsyncConnExecutorBase, query_for_session_check: str
    ) -> None:
        with self.check_closing_sql_sessions(db=db):
            for _i in range(5):
                await async_connection_executor.execute(ConnExecutorQuery(query=query_for_session_check))

    async def test_get_table_schema_info(
        self,
        async_connection_executor: AsyncConnExecutorBase,
        existing_table_ident: TableIdent,
    ) -> None:
        # Just tests that the adapter can successfully retrieve the schema.
        # Data source tests check this in more detail
        detected_columns = (
            await async_connection_executor.get_table_schema_info(table_def=existing_table_ident)
        ).schema
        assert len(detected_columns) > 0

    async def test_get_table_schema_info_for_nonexistent_table(
        self,
        async_connection_executor: AsyncConnExecutorBase,
        nonexistent_table_ident: TableIdent,
    ) -> None:
        with pytest.raises(core_exc.DLBaseException):
            await async_connection_executor.get_table_schema_info(table_def=nonexistent_table_ident)
