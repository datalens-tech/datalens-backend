from dl_api_lib_testing.connector.data_api_suites import (
    DefaultConnectorDataDistinctTestSuite,
    DefaultConnectorDataGroupByFormulaTestSuite,
    DefaultConnectorDataPreviewTestSuite,
    DefaultConnectorDataRangeTestSuite,
    DefaultConnectorDataResultTestSuite,
)
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_greenplum_tests.db.api.base import GreenplumDataApiTestBase


class TestGreenplumDataResult(GreenplumDataApiTestBase, DefaultConnectorDataResultTestSuite):
    pass


class TestGreenplumDataGroupBy(GreenplumDataApiTestBase, DefaultConnectorDataGroupByFormulaTestSuite):
    pass


class TestGreenplumDataRange(GreenplumDataApiTestBase, DefaultConnectorDataRangeTestSuite):
    pass


class TestGreenplumDataDistinct(GreenplumDataApiTestBase, DefaultConnectorDataDistinctTestSuite):
    pass


class TestGreenplumDataPreview(GreenplumDataApiTestBase, DefaultConnectorDataPreviewTestSuite):
    pass
