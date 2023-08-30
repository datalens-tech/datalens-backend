from bi_formula.connectors.base.testing.functions_markup import (
    DefaultMarkupFunctionFormulaConnectorTestSuite,
)

from bi_connector_oracle_tests.db.formula.base import (
    OracleTestBase,
)


class TestMarkupFunctionOracle(OracleTestBase, DefaultMarkupFunctionFormulaConnectorTestSuite):
    pass
