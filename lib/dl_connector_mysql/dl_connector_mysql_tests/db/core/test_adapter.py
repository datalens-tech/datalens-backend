from dl_core_testing.testcases.adapter import BaseAsyncAdapterTestClass
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_mysql.core.async_adapters_mysql import AsyncMySQLAdapter
from dl_connector_mysql.core.target_dto import MySQLConnTargetDTO
from dl_connector_mysql_tests.db.core.base import (
    BaseMySQLTestClass,
    BaseSslMySQLTestClass,
)


class TestAsyncMySQLAdapter(
    BaseMySQLTestClass,
    BaseAsyncAdapterTestClass[MySQLConnTargetDTO],
):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            BaseAsyncAdapterTestClass.test_default_pass_db_query_to_user: "Not relevant",
        },
    )

    ASYNC_ADAPTER_CLS = AsyncMySQLAdapter


class TestAsyncSslMySQLAdapter(
    BaseSslMySQLTestClass,
    BaseAsyncAdapterTestClass[MySQLConnTargetDTO],
):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            BaseAsyncAdapterTestClass.test_default_pass_db_query_to_user: "Not relevant",
        },
    )

    ASYNC_ADAPTER_CLS = AsyncMySQLAdapter
