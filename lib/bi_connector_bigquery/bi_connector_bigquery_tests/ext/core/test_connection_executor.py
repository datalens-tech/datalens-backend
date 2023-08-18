import pytest

from bi_constants.enums import BIType

from bi_core.connection_models.common_models import DBIdent

from bi_core_testing.database import C, Db
from bi_core_testing.testcases.connection_executor import (
    DefaultSyncAsyncConnectionExecutorCheckBase,
    DefaultSyncConnectionExecutorTestSuite, DefaultAsyncConnectionExecutorTestSuite,
)
from bi_connector_bigquery.core.us_connection import ConnectionSQLBigQuery

from bi_connector_bigquery_tests.ext.core.base import BaseBigQueryTestClass


class BigQuerySyncAsyncConnectionExecutorCheckBase(
        BaseBigQueryTestClass,
        DefaultSyncAsyncConnectionExecutorCheckBase[ConnectionSQLBigQuery],
):
    do_check_select_nonexistent_source = False  # FIXME: Learn to detect this error
    do_check_closing_sql_sessions = False

    @pytest.fixture(scope='class')
    def db_table_columns(self, db: Db) -> list[C]:
        return [
            col_spec
            for col_spec in C.full_house()
            if col_spec.user_type not in (
                BIType.uuid,  # UUID is not supported
                BIType.datetime, BIType.genericdatetime  # datetimes with fractional seconds are not supported  # FIXME
            )
        ]

    @pytest.fixture(scope='function')
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
