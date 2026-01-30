from dl_core_testing.testcases.adapter import BaseAsyncAdapterTestClass
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_starrocks.core.async_adapters_starrocks import AsyncStarRocksAdapter
from dl_connector_starrocks_tests.db.core.base import BaseStarRocksTestClass


class TestStarRocksAdapter(BaseStarRocksTestClass, BaseAsyncAdapterTestClass):
    ASYNC_ADAPTER_CLS = AsyncStarRocksAdapter

    test_params = RegulatedTestParams(
        mark_tests_failed={
            BaseAsyncAdapterTestClass.test_pass_db_query_to_user: "TODO: fix",
            BaseAsyncAdapterTestClass.test_default_pass_db_query_to_user: "TODO: fix",
            BaseAsyncAdapterTestClass.test_timeout: "TODO: fix",
        }
    )
