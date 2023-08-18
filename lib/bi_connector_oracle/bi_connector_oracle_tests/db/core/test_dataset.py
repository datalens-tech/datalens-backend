from bi_connector_oracle.core.constants import SOURCE_TYPE_ORACLE_TABLE
from bi_connector_oracle.core.us_connection import ConnectionSQLOracle

from bi_connector_oracle_tests.db.core.base import BaseOracleTestClass

from bi_core_testing.testcases.dataset import DefaultDatasetTestSuite


class TestOracleDataset(BaseOracleTestClass, DefaultDatasetTestSuite[ConnectionSQLOracle]):
    source_type = SOURCE_TYPE_ORACLE_TABLE
    do_check_param_hash = False  # FIXME: db_name in dsrc
