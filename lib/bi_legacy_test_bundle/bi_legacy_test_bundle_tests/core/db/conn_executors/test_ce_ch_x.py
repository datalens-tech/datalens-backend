from __future__ import annotations

import pytest

from bi_connector_clickhouse.core.clickhouse_base.connection_executors import (
    ClickHouseSyncAdapterConnExecutor, ClickHouseAsyncAdapterConnExecutor,
)

from bi_legacy_test_bundle_tests.core.common_ce_ch import BaseClickHouseTestSet


class TestClickHouseSyncAdapterConnExecutor(BaseClickHouseTestSet):
    executor_cls = ClickHouseSyncAdapterConnExecutor


class TestClickHouseAsyncAdapterExecutor(BaseClickHouseTestSet):
    executor_cls = ClickHouseAsyncAdapterConnExecutor
    inf_val = None

    @pytest.mark.skip()
    def test_type_discovery_sync(self, sync_exec_wrapper, all_supported_types_test_case):
        pass

    @pytest.mark.skip()
    def test_indexes_discovery(self, sync_exec_wrapper, index_test_case):
        pass

    @pytest.mark.skip()
    async def test_get_db_version(self, executor):
        pass

    @pytest.mark.skip()
    def test_get_db_version_sync(self, sync_exec_wrapper):
        pass

    @pytest.mark.skip()
    async def test_get_table_names(self, executor, get_table_names_test_case):
        pass

    @pytest.mark.skip()
    def test_get_table_names_sync(self, sync_exec_wrapper, get_table_names_test_case):
        pass

    @pytest.mark.skip()
    async def test_table_exists(self, executor, is_table_exists_test_case):
        pass

    @pytest.mark.skip()
    def test_table_exists_sync(self, sync_exec_wrapper, is_table_exists_test_case):
        pass
