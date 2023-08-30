from bi_api_lib_testing.connector.connection_suite import DefaultConnectorConnectionTestSuite

from bi_connector_promql_tests.db.api.base import PromQLConnectionTestBase


class TestPromQLConnection(PromQLConnectionTestBase, DefaultConnectorConnectionTestSuite):
    pass
