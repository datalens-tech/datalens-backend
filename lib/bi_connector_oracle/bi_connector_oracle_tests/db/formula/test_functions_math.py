from bi_formula.connectors.base.testing.functions_math import (
    DefaultMathFunctionFormulaConnectorTestSuite,
)

from bi_connector_oracle_tests.db.formula.base import (
    OracleTestBase,
)


class TestMathFunctionOracle(OracleTestBase, DefaultMathFunctionFormulaConnectorTestSuite):
    supports_atan_2_in_origin = False
