from dl_core_testing.testcases.dataset import DefaultDatasetTestSuite
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_mysql.core.constants import SOURCE_TYPE_MYSQL_TABLE
from dl_connector_mysql.core.us_connection import ConnectionMySQL
from dl_connector_mysql_tests.db.core.base import BaseMySQLTestClass


class TestMySQLDataset(BaseMySQLTestClass, DefaultDatasetTestSuite[ConnectionMySQL]):
    source_type = SOURCE_TYPE_MYSQL_TABLE

    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultDatasetTestSuite.test_get_param_hash: "db_name in dsrc",  # TODO: FIXME
        },
    )
