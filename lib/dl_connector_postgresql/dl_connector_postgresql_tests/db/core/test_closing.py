from dl_core_testing.testcases.closing import DefaultClosingTestSuite

from dl_connector_postgresql.core.postgresql.us_connection import ConnectionPostgreSQL
from dl_connector_postgresql.core.postgresql_base.adapters_postgres import PostgresAdapter
from dl_connector_postgresql_tests.db.core.base import BasePostgreSQLTestClass


class TestPostgreSQLClosing(
    BasePostgreSQLTestClass,
    DefaultClosingTestSuite[ConnectionPostgreSQL],
):
    SYNC_ADAPTER_CLS = PostgresAdapter
