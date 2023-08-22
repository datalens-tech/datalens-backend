from bi_connector_mysql.core.constants import SOURCE_TYPE_MYSQL_TABLE
from bi_connector_mysql.core.us_connection import ConnectionMySQL

from bi_connector_mysql_tests.db.core.base import BaseMySQLTestClass

from bi_testing.regulated_test import RegulatedTestParams
from bi_core_testing.testcases.dataset import DefaultDatasetTestSuite


class TestMySQLDataset(BaseMySQLTestClass, DefaultDatasetTestSuite[ConnectionMySQL]):
    source_type = SOURCE_TYPE_MYSQL_TABLE

    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultDatasetTestSuite.test_get_param_hash: 'db_name in dsrc',  # TODO: FIXME
        },
    )
