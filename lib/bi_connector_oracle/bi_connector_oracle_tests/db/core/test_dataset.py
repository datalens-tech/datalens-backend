from dl_core_testing.testcases.dataset import DefaultDatasetTestSuite
from dl_testing.regulated_test import RegulatedTestParams

from bi_connector_oracle.core.constants import SOURCE_TYPE_ORACLE_TABLE
from bi_connector_oracle.core.us_connection import ConnectionSQLOracle
from bi_connector_oracle_tests.db.core.base import BaseOracleTestClass


class TestOracleDataset(BaseOracleTestClass, DefaultDatasetTestSuite[ConnectionSQLOracle]):
    source_type = SOURCE_TYPE_ORACLE_TABLE

    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultDatasetTestSuite.test_get_param_hash: "db_name in dsrc",  # TODO: FIXME
        },
    )
