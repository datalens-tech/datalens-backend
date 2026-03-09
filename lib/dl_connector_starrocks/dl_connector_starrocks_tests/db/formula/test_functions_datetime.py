from dl_formula_testing.testcases.functions_datetime import DefaultDateTimeFunctionFormulaConnectorTestSuite

from dl_connector_starrocks_tests.db.formula.base import StarRocksTestBase


class TestDateTimeFunctionStarRocks(StarRocksTestBase, DefaultDateTimeFunctionFormulaConnectorTestSuite):
    supports_deprecated_dateadd = False
    supports_deprecated_datepart_2 = False
    supports_datetimetz = False
