from bi_connector_snowflake_tests.ext.formula.base import SnowFlakeTestBase  # noqa
from bi_formula.connectors.base.testing.operators import (
    DefaultOperatorFormulaConnectorTestSuite,
)


class TestOperatorSnowFlake(SnowFlakeTestBase, DefaultOperatorFormulaConnectorTestSuite):
    pass
