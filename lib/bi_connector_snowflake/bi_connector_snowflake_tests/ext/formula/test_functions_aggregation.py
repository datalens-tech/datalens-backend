from bi_connector_snowflake_tests.ext.formula.base import SnowFlakeTestBase  # noqa
from bi_formula.connectors.base.testing.functions_aggregation import (
    DefaultMainAggFunctionFormulaConnectorTestSuite,
)


class TestMainAggFunctionSnowFlake(SnowFlakeTestBase, DefaultMainAggFunctionFormulaConnectorTestSuite):
    supports_countd_approx = True
    supports_quantile = True
    supports_median = True
