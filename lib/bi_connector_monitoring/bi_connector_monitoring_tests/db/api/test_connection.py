from dl_api_lib_testing.connector.connection_suite import DefaultConnectorConnectionTestSuite
from dl_testing.regulated_test import RegulatedTestParams

from bi_connector_monitoring_tests.db.api.base import MonitoringConnectionTestBase


class TestMonitoringConnection(MonitoringConnectionTestBase, DefaultConnectorConnectionTestSuite):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultConnectorConnectionTestSuite.test_cache_ttl_sec_override: "Unavailable for Monitoring",
            DefaultConnectorConnectionTestSuite.test_test_connection: "Won't work with fake connection parameters",
        }
    )
