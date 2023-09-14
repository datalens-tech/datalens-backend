from bi_formula_testing.testcases.functions_aggregation import DefaultMainAggFunctionFormulaConnectorTestSuite

from bi_connector_snowflake_tests.ext.formula.base import SnowFlakeTestBase  # noqa


class TestMainAggFunctionSnowFlake(SnowFlakeTestBase, DefaultMainAggFunctionFormulaConnectorTestSuite):
    supports_countd_approx = True
    supports_quantile = True
    supports_median = True
