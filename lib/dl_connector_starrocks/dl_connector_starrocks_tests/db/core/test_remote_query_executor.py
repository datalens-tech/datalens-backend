import pytest

from dl_core_testing.testcases.remote_query_executor import BaseRemoteQueryExecutorTestClass

from dl_connector_starrocks.core.adapters_starrocks import StarRocksAdapter
from dl_connector_starrocks.core.async_adapters_starrocks import AsyncStarRocksAdapter
from dl_connector_starrocks_tests.db.core.base import BaseStarRocksTestClass


@pytest.mark.skip(reason="Remote query executor not yet configured for StarRocks")
class TestStarRocksRemoteQueryExecutor(BaseStarRocksTestClass, BaseRemoteQueryExecutorTestClass):
    SYNC_ADAPTER_CLS = StarRocksAdapter
    ASYNC_ADAPTER_CLS = AsyncStarRocksAdapter
