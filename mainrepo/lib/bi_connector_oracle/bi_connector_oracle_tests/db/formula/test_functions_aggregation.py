from bi_formula.connectors.base.testing.functions_aggregation import (
    DefaultMainAggFunctionFormulaConnectorTestSuite,
)

from bi_connector_oracle_tests.db.formula.base import (
    OracleTestBase,
)


class TestMainAggFunctionOracle(OracleTestBase, DefaultMainAggFunctionFormulaConnectorTestSuite):
    supports_countd_approx = True
    supports_quantile = True
    supports_median = True
