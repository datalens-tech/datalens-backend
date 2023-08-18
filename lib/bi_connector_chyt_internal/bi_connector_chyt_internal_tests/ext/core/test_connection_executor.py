from typing import TypeVar

from bi_core.us_connection_base import ConnectionBase

from bi_core_testing.testcases.connection_executor import (
    DefaultSyncConnectionExecutorTestSuite, DefaultAsyncConnectionExecutorTestSuite,
)

from bi_connector_chyt_internal.core.us_connection import ConnectionCHYTInternalToken, ConnectionCHYTUserAuth

from bi_connector_chyt_internal_tests.ext.core.base import BaseCHYTTestClass, BaseCHYTUserAuthTestClass
from bi_connector_chyt_internal_tests.ext.core.ce_base import CHYTCommonSyncAsyncConnectionExecutorCheckBase


_CONN_TV = TypeVar('_CONN_TV', bound=ConnectionBase)


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
    do_check_db_version = False
    do_check_table_exists = False
    do_check_table_not_exists = False


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
    do_check_db_version = False
    do_check_table_exists = False
    do_check_table_not_exists = False
