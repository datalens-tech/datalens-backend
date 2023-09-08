from bi_connector_snowflake_tests.ext.formula.base import SnowFlakeTestBase  # noqa
from bi_formula_testing.testcases.operators import (
    DefaultOperatorFormulaConnectorTestSuite,
)


class TestOperatorSnowFlake(SnowFlakeTestBase, DefaultOperatorFormulaConnectorTestSuite):
    pass
