from dl_api_lib_testing.connector.connection_suite import DefaultConnectorConnectionTestSuite

from dl_connector_mysql_tests.db.api.base import MySQLConnectionTestBase


class TestMySQLConnection(MySQLConnectionTestBase, DefaultConnectorConnectionTestSuite):
    pass
