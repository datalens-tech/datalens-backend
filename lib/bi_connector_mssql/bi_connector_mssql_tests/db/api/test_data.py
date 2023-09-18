from dl_api_lib_testing.connector.data_api_suites import (
    DefaultConnectorDataDistinctTestSuite,
    DefaultConnectorDataGroupByFormulaTestSuite,
    DefaultConnectorDataRangeTestSuite,
    DefaultConnectorDataResultTestSuite,
)

from bi_connector_mssql_tests.db.api.base import MSSQLDataApiTestBase


class TestMSSQLDataResult(MSSQLDataApiTestBase, DefaultConnectorDataResultTestSuite):
    pass


class TestMSSQLDataGroupBy(MSSQLDataApiTestBase, DefaultConnectorDataGroupByFormulaTestSuite):
    pass


class TestMSSQLDataRange(MSSQLDataApiTestBase, DefaultConnectorDataRangeTestSuite):
    pass


class TestMSSQLDataDistinct(MSSQLDataApiTestBase, DefaultConnectorDataDistinctTestSuite):
    pass
