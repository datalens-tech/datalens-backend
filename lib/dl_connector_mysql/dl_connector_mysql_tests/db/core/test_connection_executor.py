from typing import (
    Optional,
    Sequence,
)

from aiomysql.sa.result import (
    ResultMetaData,
    ResultProxy,
)
import pytest
from sqlalchemy.dialects import mysql as mysql_types

from dl_constants.enums import UserDataType
from dl_core.connection_models.common_models import DBIdent
from dl_core_testing.testcases.connection_executor import (
    DefaultAsyncConnectionExecutorTestSuite,
    DefaultSyncAsyncConnectionExecutorCheckBase,
    DefaultSyncConnectionExecutorTestSuite,
)
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_mysql.core.us_connection import ConnectionMySQL
from dl_connector_mysql_tests.db.config import CoreConnectionSettings
from dl_connector_mysql_tests.db.core.base import (
    BaseMySQLTestClass,
)


class MySQLSyncAsyncConnectionExecutorCheckBase(
    BaseMySQLTestClass,
    DefaultSyncAsyncConnectionExecutorCheckBase[ConnectionMySQL],
):
    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultAsyncConnectionExecutorTestSuite.test_closing_sql_sessions: "Not implemented",  # TODO: FIXME
        },
    )

    @pytest.fixture(scope="function")
    def db_ident(self) -> DBIdent:
        return DBIdent(db_name=CoreConnectionSettings.DB_NAME)

    def check_db_version(self, db_version: Optional[str]) -> None:
        assert db_version is not None
        assert "." in db_version


class TestMySQLSyncConnectionExecutor(
    MySQLSyncAsyncConnectionExecutorCheckBase,
    DefaultSyncConnectionExecutorTestSuite[ConnectionMySQL],
):
    def get_schemas_for_type_recognition(self) -> dict[str, Sequence[DefaultSyncConnectionExecutorTestSuite.CD]]:
        return {
            "mysql_types_number": [
                self.CD(mysql_types.TINYINT(), UserDataType.integer),
                self.CD(mysql_types.SMALLINT(), UserDataType.integer),
                self.CD(mysql_types.MEDIUMINT(), UserDataType.integer),
                self.CD(mysql_types.INTEGER(), UserDataType.integer),
                self.CD(mysql_types.BIGINT(), UserDataType.integer),
                self.CD(mysql_types.FLOAT(), UserDataType.float),
                self.CD(mysql_types.DOUBLE(), UserDataType.float),
                self.CD(mysql_types.NUMERIC(), UserDataType.float, nt_name="decimal"),
                self.CD(mysql_types.DECIMAL(), UserDataType.float),
                self.CD(mysql_types.BIT(1), UserDataType.boolean),
            ],
            "mysql_types_string": [
                self.CD(mysql_types.CHAR(), UserDataType.string),
                self.CD(mysql_types.VARCHAR(100), UserDataType.string),
                self.CD(mysql_types.TINYTEXT(), UserDataType.string),
                self.CD(mysql_types.TEXT(), UserDataType.string),
            ],
            "mysql_types_date": [
                self.CD(mysql_types.DATE(), UserDataType.date),
                self.CD(mysql_types.TIMESTAMP(), UserDataType.genericdatetime),
                self.CD(mysql_types.DATETIME(), UserDataType.genericdatetime),
            ],
            "mysql_types_other": [
                self.CD(mysql_types.TINYBLOB(), UserDataType.string),
                self.CD(mysql_types.BLOB(), UserDataType.string),
                self.CD(mysql_types.BINARY(), UserDataType.string),
                self.CD(mysql_types.VARBINARY(100), UserDataType.string),
                self.CD(mysql_types.ENUM("a", "b", "c", name="some_enum"), UserDataType.string),
            ],
        }


class TestMySQLAsyncConnectionExecutor(
    MySQLSyncAsyncConnectionExecutorCheckBase,
    DefaultAsyncConnectionExecutorTestSuite[ConnectionMySQL],
):
    @pytest.fixture(autouse=True, scope="function")
    def mock_aiomysql_prepare(self, monkeypatch):
        """
        Replace for
        https://github.com/aio-libs/aiomysql/blob/83aa96e12b1b3f2bd373f60a9c585b6e73f40f52/aiomysql/sa/result.py#L253
        The ResultProxy._prepare creates destructor for an internal cursor object by `self._weak = weakref.ref(self, callback)`
        However, there's an issue where it attempts to execute a task in a potentially closed event loop,
        leading to strange behavior:
        1. Datalens reads all data from ReadProxy and finish the test
        2. fixture event_loop closes the loop
        3. GC deletes old objects -> runs weakref's callbacks
        2. the cursor's callback creates coroutine Cursor.close
        3. the cursor's callback tries to run this coroutine in the old loop (2)
        4. the old loop generates exception, but the coroutine is still here
        5. asyncio generates warning
        5. aiohttp-pytest-plugin transforms warning to exception

        There is no way to control a weakref's callback, and also we can't delete a weakref.
        Anyway, aiomysql closes cursor along with engine and connection ->
        skipping a destructor in tests doesn't sound like a terrible idea
        """

        async def _prepare(self, *args, **kwargs):
            cursor = self._cursor
            self._weak = None
            if cursor.description is not None:
                self._metadata = ResultMetaData(self, cursor.description)
            else:
                self._metadata = None
                await self.close()

        monkeypatch.setattr(
            ResultProxy,
            "_prepare",
            _prepare,
        )

    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultAsyncConnectionExecutorTestSuite.test_table_exists: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_table_not_exists: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_get_table_schema_info: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_get_table_schema_info_for_nonexistent_table: "Not implemented",
        },
    )


# # @pytest.mark.skipif(os.environ.get("WE_ARE_IN_CI"), reason="can't use localhost")
# class TestSslMySQLSyncConnectionExecutor(
#     BaseSslMySQLTestClass,
#     TestMySQLSyncConnectionExecutor,
# ):
#     def test_test(self, sync_connection_executor: SyncConnExecutorBase) -> None:
#         super().test_test(sync_connection_executor)
#         self.check_ssl_directory_is_empty()


# # @pytest.mark.skipif(os.environ.get("WE_ARE_IN_CI"), reason="can't use localhost")
# class TestSslMySQLAsyncConnectionExecutor(
#     BaseSslMySQLTestClass,
#     TestMySQLAsyncConnectionExecutor,
# ):
#     async def test_test(self, async_connection_executor: AsyncConnExecutorBase) -> None:
#         await super().test_test(async_connection_executor)
#         await asyncio.sleep(0.1)
#         self.check_ssl_directory_is_empty()

#     async def test_multiple_connection_test(self, async_connection_executor: AsyncConnExecutorBase) -> None:
#         await super().test_multiple_connection_test(async_connection_executor)
#         await asyncio.sleep(0.1)
#         self.check_ssl_directory_is_empty()
