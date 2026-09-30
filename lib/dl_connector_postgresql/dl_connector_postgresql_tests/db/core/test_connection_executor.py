import asyncio
from collections.abc import Sequence
import uuid

import pytest
import shortuuid
from sqlalchemy.dialects import postgresql as pg_types

from dl_constants import UserDataType
from dl_core.connection_executors import AsyncConnExecutorBase
from dl_core.connection_executors.sync_base import SyncConnExecutorBase
from dl_core.connection_models.common_models import (
    DBIdent,
    TableIdent,
)
from dl_core.db import IndexInfo
from dl_core_testing.database import (
    Db,
    make_schema,
)
from dl_core_testing.testcases.connection_executor import (
    DefaultAsyncConnectionExecutorTestSuite,
    DefaultIndexDiscoveryTestSuite,
    DefaultSchemaListingTestSuite,
    DefaultSyncAsyncConnectionExecutorCheckBase,
    DefaultSyncConnectionExecutorTestSuite,
    IndexTestCase,
    SchemaNamesTestCase,
)
from dl_sqlalchemy_postgres.base import CITEXT
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_postgresql.core.postgresql.us_connection import ConnectionPostgreSQL
from dl_connector_postgresql_tests.db.config import CoreConnectionSettings
from dl_connector_postgresql_tests.db.core.base import (
    BasePostgreSQLTestClass,
    BaseSslPostgreSQLTestClass,
)


class PostgreSQLSyncAsyncConnectionExecutorCheckBase(
    BasePostgreSQLTestClass,
    DefaultSyncAsyncConnectionExecutorCheckBase[ConnectionPostgreSQL],
):
    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultAsyncConnectionExecutorTestSuite.test_closing_sql_sessions: "",  # TODO: FIXME
        },
    )

    @pytest.fixture
    def db_ident(self) -> DBIdent:
        return DBIdent(db_name=CoreConnectionSettings.DB_NAME)

    def check_db_version(self, db_version: str | None) -> None:
        assert db_version is not None
        assert "." in db_version

    @pytest.fixture
    def enabled_citext_extension(self, db: Db) -> None:
        db.execute("CREATE EXTENSION IF NOT EXISTS CITEXT;")

    @pytest.fixture
    def schema_names_test_case(self, db: Db) -> SchemaNamesTestCase:
        schema_name_list = [make_schema(db) for _ in range(3)]
        return SchemaNamesTestCase(
            target_db_ident=DBIdent(None),
            expected_schemas=schema_name_list,
            full_match_required=False,
        )


@pytest.mark.usefixtures("enabled_citext_extension")
class TestPostgreSQLSyncConnectionExecutor(
    PostgreSQLSyncAsyncConnectionExecutorCheckBase,
    DefaultSyncConnectionExecutorTestSuite[ConnectionPostgreSQL],
    DefaultSchemaListingTestSuite[ConnectionPostgreSQL],
    DefaultIndexDiscoveryTestSuite[ConnectionPostgreSQL],
):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultAsyncConnectionExecutorTestSuite.test_table_exists: "",  # TODO: FIXME
        },
    )

    @pytest.fixture(params=["no_idx", "one_columns_sorting", "two_columns_sorting"])
    def index_test_case(self, db: Db, request: pytest.FixtureRequest):
        table_name = f"idx_test_{shortuuid.uuid()}"
        cases = {
            "no_idx": (
                [f'CREATE TABLE "{table_name}" (num_1 integer, num_2 integer, txt text)'],
                [],
            ),
            "one_columns_sorting": (
                [
                    f'CREATE TABLE "{table_name}" (num_1 integer, num_2 integer, txt text)',
                    f'CREATE INDEX ON "{table_name}" (num_1)',
                ],
                [IndexInfo(columns=("num_1",), kind=None)],
            ),
            "two_columns_sorting": (
                [
                    f'CREATE TABLE "{table_name}" (num_1 integer, num_2 integer, txt text)',
                    f'CREATE INDEX ON "{table_name}" (num_1, num_2)',
                ],
                [IndexInfo(columns=("num_1", "num_2"), kind=None)],
            ),
        }
        ddl_list, expected_indexes = cases[request.param]
        try:
            for ddl in ddl_list:
                db.execute(ddl)
            yield IndexTestCase(
                table_ident=TableIdent(db_name=None, schema_name=None, table_name=table_name),
                expected_indexes=frozenset(expected_indexes),
            )
        finally:
            db.execute(f'DROP TABLE IF EXISTS "{table_name}"')

    def get_schemas_for_type_recognition(self) -> dict[str, Sequence[DefaultSyncConnectionExecutorTestSuite.CD]]:
        return {
            "types_postgres_number": [
                self.CD(pg_types.SMALLINT(), UserDataType.integer),
                self.CD(pg_types.INTEGER(), UserDataType.integer),
                self.CD(pg_types.BIGINT(), UserDataType.integer),
                self.CD(pg_types.REAL(), UserDataType.float),
                self.CD(pg_types.DOUBLE_PRECISION(), UserDataType.float),
                self.CD(pg_types.NUMERIC(), UserDataType.float),
            ],
            "types_postgres_string": [
                self.CD(pg_types.CHAR(), UserDataType.string),
                self.CD(pg_types.VARCHAR(100), UserDataType.string),
                self.CD(pg_types.TEXT(), UserDataType.string),
                self.CD(CITEXT(), UserDataType.string),
            ],
            "types_postgres_date": [
                self.CD(pg_types.DATE(), UserDataType.date),
                self.CD(pg_types.TIMESTAMP(timezone=False), UserDataType.genericdatetime),
                self.CD(pg_types.TIMESTAMP(timezone=True), UserDataType.genericdatetime),
            ],
            "types_postgres_other": [
                self.CD(pg_types.BOOLEAN(), UserDataType.boolean),
                self.CD(pg_types.ENUM("var1", "var2", name=str(uuid.uuid4())), UserDataType.string),
            ],
            "types_postgres_array": [
                self.CD(pg_types.ARRAY(pg_types.SMALLINT()), UserDataType.array_int),
                self.CD(pg_types.ARRAY(pg_types.INTEGER()), UserDataType.array_int),
                self.CD(pg_types.ARRAY(pg_types.BIGINT()), UserDataType.array_int),
                self.CD(pg_types.ARRAY(pg_types.REAL()), UserDataType.array_float),
                self.CD(pg_types.ARRAY(pg_types.DOUBLE_PRECISION()), UserDataType.array_float),
                self.CD(pg_types.ARRAY(pg_types.NUMERIC()), UserDataType.array_float),
                self.CD(pg_types.ARRAY(pg_types.CHAR()), UserDataType.array_str),
                self.CD(pg_types.ARRAY(pg_types.VARCHAR(100)), UserDataType.array_str),
                self.CD(pg_types.ARRAY(pg_types.TEXT()), UserDataType.array_str),
            ],
        }


class TestPostgreSQLAsyncConnectionExecutor(
    PostgreSQLSyncAsyncConnectionExecutorCheckBase,
    DefaultAsyncConnectionExecutorTestSuite[ConnectionPostgreSQL],
):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultAsyncConnectionExecutorTestSuite.test_table_exists: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_table_not_exists: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_get_table_schema_info: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_get_table_schema_info_for_nonexistent_table: "Not implemented",
        },
    )


class TestSslPostgreSQLSyncConnectionExecutor(
    BaseSslPostgreSQLTestClass,
    TestPostgreSQLSyncConnectionExecutor,
):
    def test_test(self, sync_connection_executor: SyncConnExecutorBase) -> None:
        super().test_test(sync_connection_executor)
        self.check_ssl_directory_is_empty()


class TestSslPostgreSQLAsyncConnectionExecutor(
    BaseSslPostgreSQLTestClass,
    TestPostgreSQLAsyncConnectionExecutor,
):
    @pytest.mark.asyncio
    async def test_test(self, async_connection_executor: AsyncConnExecutorBase) -> None:
        await super().test_test(async_connection_executor)
        await asyncio.sleep(0.1)
        self.check_ssl_directory_is_empty()

    @pytest.mark.asyncio
    async def test_multiple_connection_test(self, async_connection_executor: AsyncConnExecutorBase) -> None:
        await super().test_multiple_connection_test(async_connection_executor)
        await asyncio.sleep(0.1)
        self.check_ssl_directory_is_empty()
