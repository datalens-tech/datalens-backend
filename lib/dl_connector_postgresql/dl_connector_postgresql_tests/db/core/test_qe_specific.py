from dl_core_testing.testcases.qe_specific import DefaultQESpecificTestSuite

from dl_connector_postgresql.core.postgresql.us_connection import ConnectionPostgreSQL
from dl_connector_postgresql.core.postgresql_base.adapters_postgres import PostgresAdapter
from dl_connector_postgresql.core.postgresql_base.async_adapters_postgres import AsyncPostgresAdapter
from dl_connector_postgresql_tests.db.core.base import BasePostgreSQLTestClass


class TestPostgreSQLQESpecific(
    BasePostgreSQLTestClass,
    DefaultQESpecificTestSuite[ConnectionPostgreSQL],
):
    SYNC_ADAPTER_CLS = PostgresAdapter
    ASYNC_ADAPTER_CLS = AsyncPostgresAdapter
