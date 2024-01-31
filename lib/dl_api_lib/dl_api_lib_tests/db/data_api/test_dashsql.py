from dl_api_lib_testing.connector.dashsql_suite import DefaultDashSQLTestSuite
from dl_api_lib_tests.db.base import DefaultApiTestBase
from dl_constants.enums import RawSQLLevel


class TestDashSQL(DefaultApiTestBase, DefaultDashSQLTestSuite):
    raw_sql_level = RawSQLLevel.dashsql
