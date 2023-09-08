from bi_connector_snowflake_tests.ext.formula.base import SnowFlakeTestBase  # noqa
from bi_formula_testing.testcases.functions_string import (
    DefaultStringFunctionFormulaConnectorTestSuite,
)


class TestStringFunctionSnowFlake(SnowFlakeTestBase, DefaultStringFunctionFormulaConnectorTestSuite):
    pass
