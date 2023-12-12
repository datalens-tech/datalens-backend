from dl_api_lib_testing.connector.connection_suite import DefaultConnectorConnectionTestSuite

from dl_connector_clickhouse_tests.db.api.base import (
    ClickHouseConnectionDefaultUserTestBase,
    ClickHouseConnectionTestBase,
)


class TestClickHouseConnection(ClickHouseConnectionTestBase, DefaultConnectorConnectionTestSuite):
    pass


class TestClickHouseDefaultUserConnection(ClickHouseConnectionDefaultUserTestBase, DefaultConnectorConnectionTestSuite):
    pass
