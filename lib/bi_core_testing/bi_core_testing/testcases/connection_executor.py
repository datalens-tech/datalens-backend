from __future__ import annotations

import contextlib
from typing import Callable, Generator, Generic, Optional, TypeVar, TYPE_CHECKING, Sequence

import pytest
import shortuuid
import sqlalchemy as sa
from sqlalchemy.types import TypeEngine

from bi_constants.enums import BIType

import bi_core.exc as core_exc
from bi_core.us_connection_base import ConnectionBase
from bi_core.connection_models.common_models import DBIdent, TableIdent
from bi_core.connection_executors.common_base import ConnExecutorQuery

from bi_testing.regulated_test import RegulatedTestCase
from bi_core_testing.database import C, Db, DbTable, make_table
from bi_core_testing.testcases.connection import BaseConnectionTestClass

if TYPE_CHECKING:
    from bi_core.connection_executors.sync_base import SyncConnExecutorBase
    from bi_core.connection_executors.async_base import AsyncConnExecutorBase


_CONN_TV = TypeVar('_CONN_TV', bound=ConnectionBase)


class BaseConnectionExecutorTestClass(RegulatedTestCase, BaseConnectionTestClass[_CONN_TV], Generic[_CONN_TV]):
    @pytest.fixture(scope='function')
    def sync_connection_executor(
            self, sync_conn_executor_factory: Callable[[], SyncConnExecutorBase],
    ) -> SyncConnExecutorBase:

        return sync_conn_executor_factory()

    @pytest.fixture(scope='function')
    def async_connection_executor(
        self, async_conn_executor_factory: Callable[[], AsyncConnExecutorBase],
    ) -> AsyncConnExecutorBase:
        return async_conn_executor_factory()


class DefaultSyncAsyncConnectionExecutorCheckBase(BaseConnectionExecutorTestClass[_CONN_TV], Generic[_CONN_TV]):
    @pytest.fixture(scope='function')
    def db_ident(self) -> DBIdent:
        raise NotImplementedError

    @pytest.fixture(scope='function')
    def existing_table_ident(self, sample_table: DbTable) -> TableIdent:
        # Using sample table by default, but this can be overridden by redefining this fixture
        return TableIdent(
            db_name=sample_table.db.name,
            schema_name=sample_table.schema,
            table_name=sample_table.name,
        )

    @pytest.fixture(scope='class')
    def db_table_columns(self, db: Db) -> list[C]:
        return C.full_house()

    @pytest.fixture(scope='function')
    def db_table(self, db: Db, db_table_columns: list[C]) -> DbTable:
        return make_table(db, columns=db_table_columns)

    @pytest.fixture(scope='function')
    def nonexistent_table_ident(self, existing_table_ident: TableIdent) -> TableIdent:
        return existing_table_ident.clone(table_name=f'nonexistent_table_{shortuuid.uuid()}')

    def check_db_version(self, db_version: Optional[str]) -> None:
        pass

    @pytest.fixture(scope='class')
    def query_for_session_check(self) -> str:
        return 'SELECT 567'

    @contextlib.contextmanager
    def check_closing_sql_sessions(self, db: Db) -> Generator[None, None, None]:
        old_cnt = db.count_sql_sessions()
        yield
        new_cnt = db.count_sql_sessions()
        assert old_cnt == new_cnt, f'Expected {old_cnt} sessions, got {new_cnt}'


class DefaultSyncConnectionExecutorTestSuite(DefaultSyncAsyncConnectionExecutorCheckBase[_CONN_TV], Generic[_CONN_TV]):
    @pytest.fixture(scope='session')
    def conn_exec_factory_async_env(self) -> bool:
        return False

    def test_get_db_version(self, sync_connection_executor: SyncConnExecutorBase, db_ident: DBIdent) -> None:
        db_version = sync_connection_executor.get_db_version(db_ident)
        self.check_db_version(db_version)

    def test_test(self, sync_connection_executor: SyncConnExecutorBase) -> None:
        sync_connection_executor.test()

    def test_table_exists(
            self, sync_connection_executor: SyncConnExecutorBase, existing_table_ident: TableIdent, db_table: DbTable,
    ) -> None:
        assert sync_connection_executor.is_table_exists(existing_table_ident)

    def test_table_not_exists(
            self, sync_connection_executor: SyncConnExecutorBase, nonexistent_table_ident: TableIdent,
    ) -> None:
        assert not sync_connection_executor.is_table_exists(nonexistent_table_ident)

    def get_schemas_for_type_recognition(self) -> dict[str, Sequence[tuple[TypeEngine, BIType]]]:
        return {
            'standard_types': [
                (sa.Integer(), BIType.integer),
                (sa.Float(), BIType.float),
                (sa.String(length=256), BIType.string),
                (sa.Date(), BIType.date),
                (sa.DateTime(), BIType.genericdatetime),
            ],
        }

    def test_type_recognition(self, db: Db, sync_connection_executor: SyncConnExecutorBase) -> None:
        for schema_name, type_schema in sorted(self.get_schemas_for_type_recognition().items()):
            columns = [
                sa.Column(name=f'c_{shortuuid.uuid().lower()}', type_=sa_type)
                for sa_type, user_type in type_schema
            ]
            sa_table = db.table_from_columns(columns=columns)
            db.create_table(sa_table)
            detected_columns = sync_connection_executor.get_table_schema_info(
                table_def=TableIdent(db_name=db.name, schema_name=sa_table.schema, table_name=sa_table.name)
            ).schema
            assert len(detected_columns) == len(type_schema), f'Incorrect number of columns in schema {schema_name}'
            for col_idx, ((sa_type, user_type), detected_col) in enumerate(zip(type_schema, detected_columns)):
                assert detected_col.user_type == user_type, (
                    f'Incorrect user type detected for schema {schema_name} col #{col_idx}: '
                    f'expected {user_type.name}, got {detected_col.user_type.name}'
                )

    def test_simple_select(self, sync_connection_executor: SyncConnExecutorBase) -> None:
        query = ConnExecutorQuery(query=sa.select([sa.literal(1)]))
        result = next(iter(sync_connection_executor.execute(query).result))
        assert len(result) == 1
        assert result[0] == (1,)

    def test_error_on_select_from_nonexistent_source(
            self, db: Db, sync_connection_executor: SyncConnExecutorBase, nonexistent_table_ident: TableIdent,
    ) -> None:
        query = ConnExecutorQuery(query=f'SELECT * from {db.quote(nonexistent_table_ident.table_name)}')
        with pytest.raises(core_exc.SourceDoesNotExist):
            sync_connection_executor.execute(query)

    def test_closing_sql_sessions(
            self, db: Db, sync_connection_executor: SyncConnExecutorBase,
            query_for_session_check: str
    ) -> None:
        with self.check_closing_sql_sessions(db=db):
            for i in range(5):
                sync_connection_executor.execute(ConnExecutorQuery(query=query_for_session_check))


class DefaultAsyncConnectionExecutorTestSuite(DefaultSyncAsyncConnectionExecutorCheckBase[_CONN_TV], Generic[_CONN_TV]):
    @pytest.fixture(scope='session')
    def conn_exec_factory_async_env(self) -> bool:
        return True

    async def test_get_db_version(self, async_connection_executor: AsyncConnExecutorBase, db_ident: DBIdent) -> None:
        db_version = await async_connection_executor.get_db_version(db_ident)
        self.check_db_version(db_version)

    async def test_test(self, async_connection_executor: AsyncConnExecutorBase) -> None:
        await async_connection_executor.test()

    async def test_table_exists(
            self, async_connection_executor: AsyncConnExecutorBase, existing_table_ident: TableIdent,
    ) -> None:
        assert await async_connection_executor.is_table_exists(existing_table_ident)

    async def test_table_not_exists(
            self, async_connection_executor: AsyncConnExecutorBase, nonexistent_table_ident: TableIdent,
    ) -> None:
        assert not await async_connection_executor.is_table_exists(nonexistent_table_ident)

    async def test_simple_select(self, async_connection_executor: AsyncConnExecutorBase) -> None:
        query = ConnExecutorQuery(query=sa.select([sa.literal(1)]))
        result = await anext(aiter((await async_connection_executor.execute(query)).result))  # type: ignore  # FIXME: old arcadian mypy
        assert len(result) == 1
        assert result[0] == (1,)

    async def test_error_on_select_from_nonexistent_source(
            self, db: Db, async_connection_executor: AsyncConnExecutorBase, nonexistent_table_ident: TableIdent,
    ) -> None:
        query = ConnExecutorQuery(query=f'SELECT * from {db.quote(nonexistent_table_ident.table_name)}')
        with pytest.raises(core_exc.SourceDoesNotExist):
            await async_connection_executor.execute(query)

    async def test_closing_sql_sessions(
            self, db: Db, async_connection_executor: AsyncConnExecutorBase,
            query_for_session_check: str
    ) -> None:
        with self.check_closing_sql_sessions(db=db):
            for i in range(5):
                await async_connection_executor.execute(ConnExecutorQuery(query=query_for_session_check))
