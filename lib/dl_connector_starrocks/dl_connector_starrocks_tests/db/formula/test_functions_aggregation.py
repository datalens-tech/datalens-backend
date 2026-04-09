from dl_formula_testing.testcases.functions_aggregation import DefaultMainAggFunctionFormulaConnectorTestSuite

from dl_connector_starrocks_tests.db.formula.base import StarRocksTestBase


class TestAggFunctionsStarRocks(StarRocksTestBase, DefaultMainAggFunctionFormulaConnectorTestSuite):
    supports_any = True
    supports_countd_approx = True
    supports_arg_min_max = True
