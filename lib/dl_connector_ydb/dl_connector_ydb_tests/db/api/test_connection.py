from dl_api_lib_testing.connector.connection_suite import DefaultConnectorConnectionTestSuite

from dl_connector_ydb_tests.db.api.base import YDBConnectionTestBase


class TestYDBConnection(YDBConnectionTestBase, DefaultConnectorConnectionTestSuite):
    pass
