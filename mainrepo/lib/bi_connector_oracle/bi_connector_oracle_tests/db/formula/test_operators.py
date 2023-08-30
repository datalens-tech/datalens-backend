from bi_formula.connectors.base.testing.operators import (
    DefaultOperatorFormulaConnectorTestSuite,
)

from bi_connector_oracle_tests.db.formula.base import (
    OracleTestBase,
)


class TestOperatorOracle(OracleTestBase, DefaultOperatorFormulaConnectorTestSuite):
    pass
