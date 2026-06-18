from dl_core_testing.testcases.connection_executor import ReadWriteAdapterTestSuite
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_mysql.core.us_connection import ConnectionMySQL
from dl_connector_mysql_tests.db.core.base import BaseMySQLTestClass


class TestMySQLReadWrite(
    BaseMySQLTestClass,
    ReadWriteAdapterTestSuite[ConnectionMySQL],
):
    test_params = RegulatedTestParams(
        mark_tests_failed={
            ReadWriteAdapterTestSuite.test_write_rejected_when_not_allow_write: "Write mode not implemented for MySQL",
        },
    )
