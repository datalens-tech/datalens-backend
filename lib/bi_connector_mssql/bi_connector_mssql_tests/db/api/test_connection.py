from bi_api_lib_testing.connector.connection_suite import DefaultConnectorConnectionTestSuite

from bi_connector_mssql_tests.db.api.base import MSSQLConnectionTestBase


class TestMSSQLConnection(MSSQLConnectionTestBase, DefaultConnectorConnectionTestSuite):
    pass
