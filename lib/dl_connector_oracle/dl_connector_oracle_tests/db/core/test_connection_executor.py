from typing import (
    Optional,
    Sequence,
)

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects.oracle import base as oracle_types

from dl_constants.enums import UserDataType
from dl_core.connection_executors.sync_base import SyncConnExecutorBase
from dl_core.connection_models.common_models import (
    DBIdent,
    SchemaIdent,
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

from dl_connector_oracle.core.us_connection import ConnectionSQLOracle
from dl_connector_oracle_tests.db.config import CoreConnectionSettings
from dl_connector_oracle_tests.db.core.base import BaseOracleTestClass


class OracleSyncAsyncConnectionExecutorCheckBase(
    BaseOracleTestClass,
    DefaultSyncAsyncConnectionExecutorCheckBase[ConnectionSQLOracle],
):
    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultAsyncConnectionExecutorTestSuite.test_error_on_select_from_nonexistent_source: "",  # TODO: FIXME
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

    @pytest.fixture(scope="class")
    def query_for_session_check(self) -> str:
        return "SELECT 567 FROM DUAL"


class TestOracleSyncConnectionExecutor(
    OracleSyncAsyncConnectionExecutorCheckBase,
    DefaultSyncConnectionExecutorTestSuite[ConnectionSQLOracle],
):
    subselect_query_for_schema_test = "(SELECT 1 AS num FROM DUAL)"

    def get_schemas_for_type_recognition(self) -> dict[str, Sequence[DefaultSyncConnectionExecutorTestSuite.CD]]:
        return {
            "oracle_types_number": [
                self.CD(sa.Numeric(16, 0), UserDataType.integer, nt_name="integer"),
                self.CD(sa.Numeric(16, 8), UserDataType.float, nt_name="number"),
                self.CD(oracle_types.NUMBER(), UserDataType.integer, nt_name="integer"),
                self.CD(oracle_types.NUMBER(10, 5), UserDataType.float),
                self.CD(oracle_types.BINARY_FLOAT(), UserDataType.float),
                self.CD(oracle_types.BINARY_DOUBLE(), UserDataType.float),
            ],
            "oracle_types_string": [
                self.CD(sa.CHAR(), UserDataType.string),
                self.CD(sa.NCHAR(), UserDataType.string),
                self.CD(sa.String(length=256), UserDataType.string, nt_name="varchar"),
                self.CD(oracle_types.VARCHAR2(100), UserDataType.string, nt_name="varchar"),
                self.CD(oracle_types.NVARCHAR2(100), UserDataType.string),
            ],
            "oracle_types_date": [
                self.CD(oracle_types.DATE(), UserDataType.genericdatetime),
                self.CD(oracle_types.TIMESTAMP(), UserDataType.genericdatetime),
            ],
        }

    def test_get_table_names(
        self,
        sample_table: DbTable,
        db: Db,
        sync_connection_executor: SyncConnExecutorBase,
    ) -> None:
        # at the moment, checks that sample table is listed among the others

        tables = [sample_table]
        expected_table_names = set(table.name.upper() for table in tables)

        actual_tables = sync_connection_executor.get_tables(SchemaIdent(db_name=db.name, schema_name=None))
        actual_table_names = [tid.table_name for tid in actual_tables]

        assert set(actual_table_names).issuperset(expected_table_names)


class TestOracleAsyncConnectionExecutor(
    OracleSyncAsyncConnectionExecutorCheckBase,
    DefaultAsyncConnectionExecutorTestSuite[ConnectionSQLOracle],
):
    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultAsyncConnectionExecutorTestSuite.test_closing_sql_sessions: "Sessions not closed",  # TODO: FIXME
        },
    )
