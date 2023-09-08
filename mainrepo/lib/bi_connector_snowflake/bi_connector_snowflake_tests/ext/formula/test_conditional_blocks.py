from bi_connector_snowflake_tests.ext.formula.base import SnowFlakeTestBase  # noqa
from bi_formula_testing.testcases.conditional_blocks import (
    DefaultConditionalBlockFormulaConnectorTestSuite,
)


class TestConditionalBlockSnowFlake(SnowFlakeTestBase, DefaultConditionalBlockFormulaConnectorTestSuite):
    pass
