from typing import (
    Optional,
    Sequence,
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
from dl_connector_mysql_tests.db.core.base import BaseMySQLTestClass


class MySQLSyncAsyncConnectionExecutorCheckBase(
    BaseMySQLTestClass,
    DefaultSyncAsyncConnectionExecutorCheckBase[ConnectionMySQL],
):
    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultAsyncConnectionExecutorTestSuite.test_closing_sql_sessions: "",  # TODO: FIXME
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
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultAsyncConnectionExecutorTestSuite.test_table_exists: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_table_not_exists: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_get_table_schema_info: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_get_table_schema_info_for_nonexistent_table: "Not implemented",
        },
    )
