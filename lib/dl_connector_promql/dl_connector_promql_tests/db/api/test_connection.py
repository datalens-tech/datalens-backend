from dl_api_lib_testing.connector.connection_suite import DefaultConnectorConnectionTestSuite

from dl_connector_promql_tests.db.api.base import PromQLConnectionTestBase


class TestPromQLConnection(PromQLConnectionTestBase, DefaultConnectorConnectionTestSuite):
    pass
