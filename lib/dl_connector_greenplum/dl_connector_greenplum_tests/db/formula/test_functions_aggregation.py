from dl_formula_testing.testcases.functions_aggregation import DefaultMainAggFunctionFormulaConnectorTestSuite

from dl_connector_greenplum_tests.db.formula.base import GreenplumTestBase


class MainAggFunctionGreenplumTestSuite(DefaultMainAggFunctionFormulaConnectorTestSuite):
    supports_all_concat = True
    supports_quantile = True
    supports_median = True


class TestMainAggFunctionGreenplum(GreenplumTestBase, MainAggFunctionGreenplumTestSuite):
    pass
