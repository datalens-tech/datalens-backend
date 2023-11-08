from dl_api_lib_testing.connector.data_api_suites import (
    DefaultConnectorDataDistinctTestSuite,
    DefaultConnectorDataGroupByFormulaTestSuite,
    DefaultConnectorDataPreviewTestSuite,
    DefaultConnectorDataRangeTestSuite,
    DefaultConnectorDataResultTestSuite,
)
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_mssql_tests.db.api.base import MSSQLDataApiTestBase


class TestMSSQLDataResult(MSSQLDataApiTestBase, DefaultConnectorDataResultTestSuite):
    test_params = RegulatedTestParams(
        mark_features_skipped={
            DefaultConnectorDataResultTestSuite.array_support: "MSSQL doesn't support arrays",
        }
    )


class TestMSSQLDataGroupBy(MSSQLDataApiTestBase, DefaultConnectorDataGroupByFormulaTestSuite):
    pass


class TestMSSQLDataRange(MSSQLDataApiTestBase, DefaultConnectorDataRangeTestSuite):
    pass


class TestMSSQLDataDistinct(MSSQLDataApiTestBase, DefaultConnectorDataDistinctTestSuite):
    pass


class TestMSSQLDataPreview(MSSQLDataApiTestBase, DefaultConnectorDataPreviewTestSuite):
    pass
