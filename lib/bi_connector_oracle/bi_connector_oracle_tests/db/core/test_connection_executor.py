from typing import Optional, Sequence

import pytest
import sqlalchemy as sa
from sqlalchemy.types import TypeEngine

from bi_constants.enums import BIType

from bi_core.connection_models.common_models import DBIdent

from bi_testing.regulated_test import RegulatedTestParams
from bi_core_testing.testcases.connection_executor import (
    DefaultSyncAsyncConnectionExecutorCheckBase,
    DefaultSyncConnectionExecutorTestSuite, DefaultAsyncConnectionExecutorTestSuite,
)

from bi_connector_oracle.core.us_connection import ConnectionSQLOracle

from bi_connector_oracle_tests.db.config import CoreConnectionSettings
from bi_connector_oracle_tests.db.core.base import BaseOracleTestClass


class OracleSyncAsyncConnectionExecutorCheckBase(
        BaseOracleTestClass,
        DefaultSyncAsyncConnectionExecutorCheckBase[ConnectionSQLOracle],
):
    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultAsyncConnectionExecutorTestSuite.test_error_on_select_from_nonexistent_source: '',  # TODO: FIXME
            DefaultAsyncConnectionExecutorTestSuite.test_closing_sql_sessions: 'Sessions not closed',  # TODO: FIXME
            DefaultAsyncConnectionExecutorTestSuite.test_get_table_schema_info_for_nonexistent_table: (
                'Empty schema is returned instead of an error'),  # FIXME
        },
    )

    @pytest.fixture(scope='function')
    def db_ident(self) -> DBIdent:
        return DBIdent(db_name=CoreConnectionSettings.DB_NAME)

    def check_db_version(self, db_version: Optional[str]) -> None:
        assert db_version is not None
        assert '.' in db_version

    def get_schemas_for_type_recognition(self) -> dict[str, Sequence[tuple[TypeEngine, BIType]]]:
        return {
            'standard_types_the_oracle_way': [
                (sa.Numeric(16, 0), BIType.integer),
                (sa.Numeric(16, 8), BIType.float),
                (sa.String(length=256), BIType.string),
                (sa.Date(), BIType.genericdatetime),
                (sa.DateTime(), BIType.genericdatetime),
            ],
        }

    @pytest.fixture(scope='class')
    def query_for_session_check(self) -> str:
        return 'SELECT 567 FROM DUAL'


class TestOracleSyncConnectionExecutor(
        OracleSyncAsyncConnectionExecutorCheckBase,
        DefaultSyncConnectionExecutorTestSuite[ConnectionSQLOracle],
):
    pass


class TestOracleAsyncConnectionExecutor(
        OracleSyncAsyncConnectionExecutorCheckBase,
        DefaultAsyncConnectionExecutorTestSuite[ConnectionSQLOracle],
):
    pass
