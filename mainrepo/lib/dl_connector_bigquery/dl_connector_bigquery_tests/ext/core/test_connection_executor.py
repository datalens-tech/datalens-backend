import pytest

from dl_connector_bigquery.core.us_connection import ConnectionSQLBigQuery
from dl_connector_bigquery_tests.ext.core.base import BaseBigQueryTestClass
from dl_constants.enums import BIType
from dl_core.connection_models.common_models import DBIdent
from dl_core_testing.database import (
    C,
    Db,
)
from dl_core_testing.testcases.connection_executor import (
    DefaultAsyncConnectionExecutorTestSuite,
    DefaultSyncAsyncConnectionExecutorCheckBase,
    DefaultSyncConnectionExecutorTestSuite,
)
from dl_testing.regulated_test import RegulatedTestParams


class BigQuerySyncAsyncConnectionExecutorCheckBase(
    BaseBigQueryTestClass,
    DefaultSyncAsyncConnectionExecutorCheckBase[ConnectionSQLBigQuery],
):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultAsyncConnectionExecutorTestSuite.test_error_on_select_from_nonexistent_source: "Need to learn to detect this error",  # TODO: FIXME
            DefaultAsyncConnectionExecutorTestSuite.test_closing_sql_sessions: "Not implemented",
        },
    )

    @pytest.fixture(scope="class")
    def db_table_columns(self, db: Db) -> list[C]:
        return [
            col_spec
            for col_spec in C.full_house()
            if col_spec.user_type
            not in (
                BIType.uuid,  # UUID is not supported
                BIType.datetime,
                BIType.genericdatetime,  # datetimes with fractional seconds are not supported  # FIXME
            )
        ]

    @pytest.fixture(scope="function")
    def db_ident(self) -> DBIdent:
        return DBIdent(db_name=None)


class TestBigQuerySyncConnectionExecutor(
    BigQuerySyncAsyncConnectionExecutorCheckBase,
    DefaultSyncConnectionExecutorTestSuite[ConnectionSQLBigQuery],
):
    pass


class TestBigQueryAsyncConnectionExecutor(
    BigQuerySyncAsyncConnectionExecutorCheckBase,
    DefaultAsyncConnectionExecutorTestSuite[ConnectionSQLBigQuery],
):
    pass
