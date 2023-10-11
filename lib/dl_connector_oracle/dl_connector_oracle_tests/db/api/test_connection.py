from dl_api_lib_testing.connector.connection_suite import DefaultConnectorConnectionTestSuite

from dl_connector_oracle_tests.db.api.base import OracleConnectionTestBase


class TestOracleConnection(OracleConnectionTestBase, DefaultConnectorConnectionTestSuite):
    pass
