from dl_core_testing.testcases.connection_executor import ReadWriteAdapterTestSuite

from dl_connector_postgresql.core.postgresql.us_connection import ConnectionPostgreSQL
from dl_connector_postgresql_tests.db.core.base import BasePostgreSQLTestClass


class TestPostgreSQLReadWrite(
    BasePostgreSQLTestClass,
    ReadWriteAdapterTestSuite[ConnectionPostgreSQL],
):
    pass
