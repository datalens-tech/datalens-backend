from dl_core_testing.testcases.dataset import DefaultDatasetTestSuite
from dl_testing.regulated_test import RegulatedTestParams

from bi_connector_mssql.core.constants import SOURCE_TYPE_MSSQL_TABLE
from bi_connector_mssql.core.us_connection import ConnectionMSSQL
from bi_connector_mssql_tests.db.core.base import BaseMSSQLTestClass


class TestMSSQLDataset(BaseMSSQLTestClass, DefaultDatasetTestSuite[ConnectionMSSQL]):
    source_type = SOURCE_TYPE_MSSQL_TABLE

    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultDatasetTestSuite.test_get_param_hash: "db_name in dsrc",  # TODO: FIXME
        },
    )
