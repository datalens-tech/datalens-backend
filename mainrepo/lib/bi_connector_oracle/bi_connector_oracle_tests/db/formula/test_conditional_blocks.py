from bi_formula.connectors.base.testing.conditional_blocks import (
    DefaultConditionalBlockFormulaConnectorTestSuite,
)

from bi_connector_oracle_tests.db.formula.base import (
    OracleTestBase,
)


class TestConditionalBlockOracle(OracleTestBase, DefaultConditionalBlockFormulaConnectorTestSuite):
    pass
