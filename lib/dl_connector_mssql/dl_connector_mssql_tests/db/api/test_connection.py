from dl_api_lib_testing.connector.connection_suite import DefaultConnectorConnectionTestSuite

from dl_connector_mssql_tests.db.api.base import MSSQLConnectionTestBase


class TestMSSQLConnection(MSSQLConnectionTestBase, DefaultConnectorConnectionTestSuite):
    pass
