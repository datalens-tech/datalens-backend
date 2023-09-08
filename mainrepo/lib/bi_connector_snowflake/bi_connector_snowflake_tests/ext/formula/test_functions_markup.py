from bi_connector_snowflake_tests.ext.formula.base import SnowFlakeTestBase  # noqa
from bi_formula_testing.testcases.functions_markup import (
    DefaultMarkupFunctionFormulaConnectorTestSuite,
)


class TestMarkupFunctionSnowFlake(SnowFlakeTestBase, DefaultMarkupFunctionFormulaConnectorTestSuite):
    pass
