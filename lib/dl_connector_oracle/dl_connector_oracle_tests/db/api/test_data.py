from dl_api_lib_testing.connector.data_api_suites import (
    DefaultConnectorDataCacheTestSuite,
    DefaultConnectorDataDistinctTestSuite,
    DefaultConnectorDataGroupByFormulaTestSuite,
    DefaultConnectorDataPreviewTestSuite,
    DefaultConnectorDataRangeTestSuite,
    DefaultConnectorDataResultTestSuite,
)
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_oracle_tests.db.api.base import OracleDataApiTestBase


class TestOracleDataResult(OracleDataApiTestBase, DefaultConnectorDataResultTestSuite):
    test_params = RegulatedTestParams(
        mark_features_skipped={
            DefaultConnectorDataResultTestSuite.array_support: "Oracle doesn't support arrays",
        },
        mark_tests_failed={
            DefaultConnectorDataResultTestSuite.test_get_result_with_string_filter_operations_for_numbers: (
                "BI-4978: Need to ignore the exponent"
            )
        },
    )


class TestOracleDataGroupBy(OracleDataApiTestBase, DefaultConnectorDataGroupByFormulaTestSuite):
    pass


class TestOracleDataRange(OracleDataApiTestBase, DefaultConnectorDataRangeTestSuite):
    pass


class TestOracleDataDistinct(OracleDataApiTestBase, DefaultConnectorDataDistinctTestSuite):
    pass


class TestOracleDataPreview(OracleDataApiTestBase, DefaultConnectorDataPreviewTestSuite):
    pass


class TestOracleDataCache(OracleDataApiTestBase, DefaultConnectorDataCacheTestSuite):
    data_caches_enabled = True
