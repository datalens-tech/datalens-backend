from dl_formula_testing.testcases.functions_aggregation import DefaultMainAggFunctionFormulaConnectorTestSuite

from dl_connector_trino_tests.db.formula.base import TrinoFormulaTestBase


class TestMainAggFunctionTrino(TrinoFormulaTestBase, DefaultMainAggFunctionFormulaConnectorTestSuite):
    supports_countd_approx = True
    supports_quantile = False  # Only supports quantile_approx
    supports_median = False
    supports_arg_min_max = True
    supports_any = True
    supports_all_concat = True
    supports_top_concat = True
