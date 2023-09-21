from dl_api_lib_testing.connector.data_api_suites import (
    DefaultConnectorDataDistinctTestSuite,
    DefaultConnectorDataGroupByFormulaTestSuite,
    DefaultConnectorDataPreviewTestSuite,
    DefaultConnectorDataRangeTestSuite,
    DefaultConnectorDataResultTestSuite,
)
from dl_connector_greenplum_tests.db.api.base import GreenplumDataApiTestBase
from dl_testing.regulated_test import RegulatedTestParams


class TestGreenplumDataResult(GreenplumDataApiTestBase, DefaultConnectorDataResultTestSuite):
    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultConnectorDataResultTestSuite.test_array_not_contains_filter: "BI-4951",  # TODO: FIXME
        }
    )


class TestGreenplumDataGroupBy(GreenplumDataApiTestBase, DefaultConnectorDataGroupByFormulaTestSuite):
    pass


class TestGreenplumDataRange(GreenplumDataApiTestBase, DefaultConnectorDataRangeTestSuite):
    pass


class TestGreenplumDataDistinct(GreenplumDataApiTestBase, DefaultConnectorDataDistinctTestSuite):
    pass


class TestGreenplumDataPreview(GreenplumDataApiTestBase, DefaultConnectorDataPreviewTestSuite):
    pass
