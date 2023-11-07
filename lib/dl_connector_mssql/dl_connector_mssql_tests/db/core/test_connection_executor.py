from typing import (
    Optional,
    Sequence,
)

import pytest
from sqlalchemy.dialects import mssql as mssql_types

from dl_constants.enums import UserDataType
from dl_core.connection_models.common_models import DBIdent
from dl_core_testing.testcases.connection_executor import (
    DefaultAsyncConnectionExecutorTestSuite,
    DefaultSyncAsyncConnectionExecutorCheckBase,
    DefaultSyncConnectionExecutorTestSuite,
)
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_mssql.core.us_connection import ConnectionMSSQL
from dl_connector_mssql_tests.db.config import CoreConnectionSettings
from dl_connector_mssql_tests.db.core.base import BaseMSSQLTestClass


class MSSQLSyncAsyncConnectionExecutorCheckBase(
    BaseMSSQLTestClass,
    DefaultSyncAsyncConnectionExecutorCheckBase[ConnectionMSSQL],
):
    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultAsyncConnectionExecutorTestSuite.test_get_table_schema_info_for_nonexistent_table: (
                "Empty schema is returned instead of an error"
            ),  # FIXME
        },
    )

    @pytest.fixture(scope="function")
    def db_ident(self) -> DBIdent:
        return DBIdent(db_name=CoreConnectionSettings.DB_NAME)

    def check_db_version(self, db_version: Optional[str]) -> None:
        assert db_version is not None
        assert "." in db_version


class TestMSSQLSyncConnectionExecutor(
    MSSQLSyncAsyncConnectionExecutorCheckBase,
    DefaultSyncConnectionExecutorTestSuite[ConnectionMSSQL],
):
    def get_schemas_for_type_recognition(self) -> dict[str, Sequence[DefaultSyncConnectionExecutorTestSuite.CD]]:
        return {
            "mssql_types_number": [
                self.CD(mssql_types.TINYINT(), UserDataType.integer),
                self.CD(mssql_types.SMALLINT(), UserDataType.integer),
                self.CD(mssql_types.INTEGER(), UserDataType.integer),
                self.CD(mssql_types.BIGINT(), UserDataType.integer),
                self.CD(mssql_types.FLOAT(), UserDataType.float),
                self.CD(mssql_types.REAL(), UserDataType.float),
                self.CD(mssql_types.NUMERIC(), UserDataType.float),
                self.CD(mssql_types.DECIMAL(), UserDataType.float),
                self.CD(mssql_types.BIT(), UserDataType.boolean),
            ],
            "mssql_types_text": [
                self.CD(mssql_types.CHAR(), UserDataType.string),
                self.CD(mssql_types.VARCHAR(100), UserDataType.string),
                self.CD(mssql_types.TEXT(), UserDataType.string),
                self.CD(mssql_types.NCHAR(), UserDataType.string),
                self.CD(mssql_types.NVARCHAR(100), UserDataType.string),
                self.CD(mssql_types.NTEXT(), UserDataType.string),
            ],
            "mssql_types_date": [
                self.CD(mssql_types.DATE(), UserDataType.date),
                self.CD(mssql_types.DATETIME(), UserDataType.genericdatetime),
                self.CD(mssql_types.DATETIME2(), UserDataType.genericdatetime),
                self.CD(mssql_types.SMALLDATETIME(), UserDataType.genericdatetime),
                self.CD(mssql_types.DATETIMEOFFSET(), UserDataType.genericdatetime),
            ],
        }


class TestMSSQLAsyncConnectionExecutor(
    MSSQLSyncAsyncConnectionExecutorCheckBase,
    DefaultAsyncConnectionExecutorTestSuite[ConnectionMSSQL],
):
    pass
