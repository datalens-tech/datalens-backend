from collections.abc import Sequence
import functools
from typing import Optional

from aiomysql.sa.result import (
    ResultMetaData,
    ResultProxy,
)
import pytest
import shortuuid
import sqlalchemy as sa
from sqlalchemy.dialects import mysql as mysql_types

from dl_constants.enums import UserDataType
from dl_core.connection_executors.sync_base import SyncConnExecutorBase
from dl_core.connection_models.common_models import (
    DBIdent,
    SchemaIdent,
    TableIdent,
)
from dl_core_testing.database import (
    Db,
    DbTable,
)
from dl_core_testing.testcases.connection_executor import (
    DefaultAsyncConnectionExecutorTestSuite,
    DefaultSyncAsyncConnectionExecutorCheckBase,
    DefaultSyncConnectionExecutorTestSuite,
)
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_starrocks.core.us_connection import ConnectionStarRocks
import dl_connector_starrocks_tests.db.config as test_config
from dl_connector_starrocks_tests.db.core.base import BaseStarRocksTestClass


class StarRocksSyncAsyncConnectionExecutorCheckBase(
    BaseStarRocksTestClass,
    DefaultSyncAsyncConnectionExecutorCheckBase[ConnectionStarRocks],
):
    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultAsyncConnectionExecutorTestSuite.test_closing_sql_sessions: "Not implemented",
        },
    )

    @pytest.fixture(scope="function")
    def db_ident(self) -> DBIdent:
        return DBIdent(db_name=test_config.CoreConnectionSettings.DB_NAME)


class TestStarRocksSyncConnectionExecutor(
    StarRocksSyncAsyncConnectionExecutorCheckBase,
    DefaultSyncConnectionExecutorTestSuite[ConnectionStarRocks],
):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultSyncConnectionExecutorTestSuite.test_error_on_select_from_nonexistent_source: (
                "StarRocks connects without a default database; bare table name queries fail with 'No database selected'"
            ),
        },
    )

    def get_schemas_for_type_recognition(self) -> dict[str, Sequence[DefaultSyncConnectionExecutorTestSuite.CD]]:
        """Override to use MySQL-specific types that match StarRocks native type names"""
        return {
            "starrocks_types_number": [
                self.CD(mysql_types.INTEGER(), UserDataType.integer),
                self.CD(mysql_types.BIGINT(), UserDataType.integer),
                self.CD(mysql_types.FLOAT(), UserDataType.float),
                self.CD(mysql_types.DOUBLE(), UserDataType.float),
                self.CD(mysql_types.DECIMAL(), UserDataType.float),
            ],
            "starrocks_types_string": [
                self.CD(mysql_types.VARCHAR(100), UserDataType.string),
                # StarRocks converts TEXT to VARCHAR(65533) internally
                self.CD(mysql_types.TEXT(), UserDataType.string, nt_name="varchar"),
            ],
            "starrocks_types_date": [
                self.CD(mysql_types.DATE(), UserDataType.date),
                self.CD(mysql_types.DATETIME(), UserDataType.genericdatetime),
            ],
            "starrocks_types_other": [
                self.CD(mysql_types.BOOLEAN(), UserDataType.boolean),
                # Add a second column to avoid StarRocks single-column table restriction
                self.CD(mysql_types.INTEGER(), UserDataType.integer),
            ],
        }

    def test_get_table_names(
        self,
        sample_table: DbTable,
        sync_connection_executor: SyncConnExecutorBase,
    ) -> None:
        actual_tables = sync_connection_executor.get_tables(
            SchemaIdent(
                db_name=test_config.CoreConnectionSettings.CATALOG,
                schema_name=None,
            )
        )
        actual_table_names = {t.table_name for t in actual_tables}
        assert sample_table.name in actual_table_names

    def test_type_recognition(
        self,
        request: pytest.FixtureRequest,
        db: Db,
        sample_table_schema: Optional[str],
        sync_connection_executor: SyncConnExecutorBase,
    ) -> None:
        for type_schema in self.get_schemas_for_type_recognition().values():
            columns = [
                sa.Column(name=f"c_{shortuuid.uuid().lower()}", type_=column_data.sa_type)
                for column_data in type_schema
            ]
            sa_table = db.table_from_columns(columns=columns, schema=sample_table_schema)

            db.create_table(sa_table)
            request.addfinalizer(functools.partial(db.drop_table, sa_table))

            detected_columns = sync_connection_executor.get_table_schema_info(
                table_def=TableIdent(
                    db_name=test_config.CoreConnectionSettings.CATALOG,
                    schema_name=db.name,
                    table_name=sa_table.name,
                )
            ).schema

            assert len(detected_columns) == len(type_schema)
            for col_schema, cd in zip(detected_columns, type_schema):
                assert (
                    col_schema.user_type == cd.user_type
                ), f"Column {col_schema.name}: expected {cd.user_type}, got {col_schema.user_type}"


class TestStarRocksAsyncConnectionExecutor(
    StarRocksSyncAsyncConnectionExecutorCheckBase,
    DefaultAsyncConnectionExecutorTestSuite[ConnectionStarRocks],
):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultAsyncConnectionExecutorTestSuite.test_table_exists: (
                "Async listing methods are NotImplementedError — listing is handled by sync adapter"
            ),
            DefaultAsyncConnectionExecutorTestSuite.test_table_not_exists: (
                "Async listing methods are NotImplementedError — listing is handled by sync adapter"
            ),
            DefaultAsyncConnectionExecutorTestSuite.test_get_table_schema_info: (
                "Async listing methods are NotImplementedError — listing is handled by sync adapter"
            ),
            DefaultAsyncConnectionExecutorTestSuite.test_get_table_schema_info_for_nonexistent_table: (
                "Async listing methods are NotImplementedError — listing is handled by sync adapter"
            ),
            DefaultAsyncConnectionExecutorTestSuite.test_select_data: (
                "StarRocks connects without a default database; bare table name queries fail with 'No database selected'"
            ),
            DefaultAsyncConnectionExecutorTestSuite.test_cast_row_to_output: (
                "StarRocks connects without a default database; bare table name queries fail with 'No database selected'"
            ),
            DefaultAsyncConnectionExecutorTestSuite.test_error_on_select_from_nonexistent_source: (
                "StarRocks connects without a default database; bare table name queries fail with 'No database selected'"
            ),
        },
    )

    @pytest.fixture(autouse=True, scope="function")
    def mock_aiomysql_prepare(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Patch ResultProxy._prepare to avoid 'Event loop is closed' warnings.

        Same issue as MySQL connector: aiomysql's ResultProxy destructor tries to
        close the cursor via a weakref callback after the event loop is already closed.
        See dl_connector_mysql tests for the full explanation.
        """

        async def _prepare(self, *args, **kwargs):  # type: ignore
            cursor = self._cursor
            self._weak = None
            if cursor.description is not None:
                self._metadata = ResultMetaData(self, cursor.description)
            else:
                self._metadata = None
                await self.close()

        monkeypatch.setattr(ResultProxy, "_prepare", _prepare)
