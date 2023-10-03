from dl_api_lib_testing.connector.data_api_suites import (
    DefaultConnectorDataDistinctTestSuite,
    DefaultConnectorDataGroupByFormulaTestSuite,
    DefaultConnectorDataPreviewTestSuite,
    DefaultConnectorDataRangeTestSuite,
    DefaultConnectorDataResultTestSuite,
)
from dl_testing.regulated_test import RegulatedTestParams

from bi_connector_gsheets_tests.ext.api.base import GsheetsDataApiTestBase


class TestGsheetsDataResult(GsheetsDataApiTestBase, DefaultConnectorDataResultTestSuite):
    test_params = RegulatedTestParams(
        mark_features_skipped={
            DefaultConnectorDataResultTestSuite.array_support: "Gsheets don't support arrays",
        }
    )


class TestGsheetsDataGroupBy(GsheetsDataApiTestBase, DefaultConnectorDataGroupByFormulaTestSuite):
    pass


class TestGsheetsDataRange(GsheetsDataApiTestBase, DefaultConnectorDataRangeTestSuite):
    pass


class TestGsheetsDataDistinct(GsheetsDataApiTestBase, DefaultConnectorDataDistinctTestSuite):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultConnectorDataDistinctTestSuite.test_date_filter_distinct: "Can't create a new table in gsheets tests",
        }
    )


class TestGsheetsDataPreview(GsheetsDataApiTestBase, DefaultConnectorDataPreviewTestSuite):
    pass
