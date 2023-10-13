from dl_core_testing.testcases.adapter import BaseAsyncAdapterTestClass
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_postgresql.core.postgresql_base.async_adapters_postgres import AsyncPostgresAdapter
from dl_connector_postgresql.core.postgresql_base.target_dto import PostgresConnTargetDTO
from dl_connector_postgresql_tests.db.core.base import BasePostgreSQLTestClass


class TestAsyncPostgreSQLAdapter(
    BasePostgreSQLTestClass,
    BaseAsyncAdapterTestClass[PostgresConnTargetDTO],
):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            BaseAsyncAdapterTestClass.test_default_pass_db_query_to_user: "Not relevant",
        },
    )

    ASYNC_ADAPTER_CLS = AsyncPostgresAdapter
