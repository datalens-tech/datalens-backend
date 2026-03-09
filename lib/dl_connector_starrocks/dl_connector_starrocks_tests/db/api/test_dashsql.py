from dl_api_lib_testing.connector.dashsql_suite import DefaultDashSQLTestSuite

from dl_connector_starrocks_tests.db.api.base import StarRocksDashSQLConnectionTest


class TestStarRocksDashSQL(StarRocksDashSQLConnectionTest, DefaultDashSQLTestSuite):
    pass
