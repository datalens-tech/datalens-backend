from typing import Type

import pytest

from dl_core import exc
from dl_core.connection_executors.common_base import ConnExecutorQuery
from dl_core.connection_executors.sync_base import SyncConnExecutorBase
from dl_core.connection_executors.async_base import AsyncConnExecutorBase

from bi_connector_chyt_internal.core.us_connection import ConnectionCHYTInternalToken

from bi_connector_chyt_internal_tests.ext.core.base import (
    BaseCHYTTestClass, CHYTInvalidTokenTestClass, CHYTNoRobotAccessTestClass, CHYTNotExistsTestClass,
    BaseCHYTUserAuthTestClass,
)
from bi_connector_chyt_internal_tests.ext.core.config import TEST_TABLES
from bi_connector_chyt_internal_tests.ext.core.ce_base import CHYTCommonSyncAsyncConnectionExecutorCheckBase


def _test_error_on_execute_sync(
        sync_connection_executor: SyncConnExecutorBase,
        table: str, exception: Type[Exception],
) -> None:
    with pytest.raises(exception):
        sync_connection_executor.execute(
            ConnExecutorQuery(query=f'SELECT * FROM "{table}"'),
        )


async def _test_error_on_execute_async(
        async_connection_executor: AsyncConnExecutorBase,
        table: str, exception: Type[Exception],
) -> None:
    with pytest.raises(exception):
        await async_connection_executor.execute(
            ConnExecutorQuery(query=f'SELECT * FROM "{table}"'),
        )


class TestCHYTCliqueNotRunning(
        CHYTNotExistsTestClass,
        CHYTCommonSyncAsyncConnectionExecutorCheckBase[ConnectionCHYTInternalToken],
):
    def test_error_sync(self, sync_connection_executor: SyncConnExecutorBase):
        _test_error_on_execute_sync(
            sync_connection_executor=sync_connection_executor,
            table=TEST_TABLES['any_table'],
            exception=exc.CHYTCliqueNotExists,
        )

    async def test_error_async(self, async_connection_executor: AsyncConnExecutorBase):
        await _test_error_on_execute_async(
            async_connection_executor=async_connection_executor,
            table=TEST_TABLES['any_table'],
            exception=exc.CHYTCliqueNotExists,
        )


class TestCHYTAccessError(
        BaseCHYTTestClass,
        CHYTCommonSyncAsyncConnectionExecutorCheckBase[ConnectionCHYTInternalToken],
):
    def test_error_sync(self, sync_connection_executor: SyncConnExecutorBase):
        _test_error_on_execute_sync(
            sync_connection_executor=sync_connection_executor,
            table=TEST_TABLES['no_access_table'],
            exception=exc.CHYTTableAccessDenied,
        )

    async def test_error_async(self, async_connection_executor: AsyncConnExecutorBase):
        await _test_error_on_execute_async(
            async_connection_executor=async_connection_executor,
            table=TEST_TABLES['no_access_table'],
            exception=exc.CHYTTableAccessDenied,
        )


class TestCHYTUserAuthAccessError(
        BaseCHYTUserAuthTestClass,
        CHYTCommonSyncAsyncConnectionExecutorCheckBase[ConnectionCHYTInternalToken],
):
    def test_error_sync(self, sync_connection_executor: SyncConnExecutorBase):
        _test_error_on_execute_sync(
            sync_connection_executor=sync_connection_executor,
            table=TEST_TABLES['no_access_table'],
            exception=exc.CHYTTableAccessDenied,
        )

    async def test_error_async(self, async_connection_executor: AsyncConnExecutorBase):
        await _test_error_on_execute_async(
            async_connection_executor=async_connection_executor,
            table=TEST_TABLES['no_access_table'],
            exception=exc.CHYTTableAccessDenied,
        )


class TestCHYTCliqueAccessError(
        CHYTNoRobotAccessTestClass,
        CHYTCommonSyncAsyncConnectionExecutorCheckBase[ConnectionCHYTInternalToken],
):
    def test_error_sync(self, sync_connection_executor: SyncConnExecutorBase):
        _test_error_on_execute_sync(
            sync_connection_executor=sync_connection_executor,
            table=TEST_TABLES['no_access_table'],
            exception=exc.CHYTCliqueAccessDenied,
        )

    async def test_error_async(self, async_connection_executor: AsyncConnExecutorBase):
        await _test_error_on_execute_async(
            async_connection_executor=async_connection_executor,
            table=TEST_TABLES['no_access_table'],
            exception=exc.CHYTCliqueAccessDenied,
        )


class TestCHYTAuthError(
        CHYTInvalidTokenTestClass,
        CHYTCommonSyncAsyncConnectionExecutorCheckBase[ConnectionCHYTInternalToken],
):
    def test_error_sync(self, sync_connection_executor: SyncConnExecutorBase):
        _test_error_on_execute_sync(
            sync_connection_executor=sync_connection_executor,
            table=TEST_TABLES['any_table'],
            exception=exc.CHYTAuthError,
        )

    async def test_error_async(self, async_connection_executor: AsyncConnExecutorBase):
        await _test_error_on_execute_async(
            async_connection_executor=async_connection_executor,
            table=TEST_TABLES['any_table'],
            exception=exc.CHYTAuthError,
        )
