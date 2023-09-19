from typing import TypeVar

from dl_core.us_connection_base import ConnectionBase
from dl_core_testing.testcases.connection_executor import (
    DefaultAsyncConnectionExecutorTestSuite,
    DefaultSyncConnectionExecutorTestSuite,
)
from dl_testing.regulated_test import RegulatedTestParams

from bi_connector_chyt_internal.core.us_connection import (
    ConnectionCHYTInternalToken,
    ConnectionCHYTUserAuth,
)
from bi_connector_chyt_internal_tests.ext.core.base import (
    BaseCHYTTestClass,
    BaseCHYTUserAuthTestClass,
)
from bi_connector_chyt_internal_tests.ext.core.ce_base import CHYTCommonSyncAsyncConnectionExecutorCheckBase


_CONN_TV = TypeVar("_CONN_TV", bound=ConnectionBase)


class TestCHYTInternalTokenSyncConnectionExecutor(
    CHYTCommonSyncAsyncConnectionExecutorCheckBase[ConnectionCHYTInternalToken],
    BaseCHYTTestClass,
    DefaultSyncConnectionExecutorTestSuite[ConnectionCHYTInternalToken],
):
    pass


class TestCHYTInternalTokenAsyncConnectionExecutor(
    CHYTCommonSyncAsyncConnectionExecutorCheckBase[ConnectionCHYTInternalToken],
    BaseCHYTTestClass,
    DefaultAsyncConnectionExecutorTestSuite[ConnectionCHYTInternalToken],
):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultAsyncConnectionExecutorTestSuite.test_get_db_version: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_table_exists: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_table_not_exists: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_get_table_schema_info: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_get_table_schema_info_for_nonexistent_table: "Not implemented",
        },
    )


class TestCHYTUserAuthSyncConnectionExecutor(
    CHYTCommonSyncAsyncConnectionExecutorCheckBase[ConnectionCHYTUserAuth],
    BaseCHYTUserAuthTestClass,
    DefaultSyncConnectionExecutorTestSuite[ConnectionCHYTUserAuth],
):
    pass


class TestCHYTUserAuthAsyncConnectionExecutor(
    CHYTCommonSyncAsyncConnectionExecutorCheckBase[ConnectionCHYTUserAuth],
    BaseCHYTUserAuthTestClass,
    DefaultAsyncConnectionExecutorTestSuite[ConnectionCHYTUserAuth],
):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultAsyncConnectionExecutorTestSuite.test_get_db_version: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_table_exists: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_table_not_exists: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_get_table_schema_info: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_get_table_schema_info_for_nonexistent_table: "Not implemented",
        },
    )
