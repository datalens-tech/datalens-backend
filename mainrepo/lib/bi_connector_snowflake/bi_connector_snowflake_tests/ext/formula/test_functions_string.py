from bi_formula_testing.testcases.functions_string import DefaultStringFunctionFormulaConnectorTestSuite

from bi_connector_snowflake_tests.ext.formula.base import SnowFlakeTestBase  # noqa


class TestStringFunctionSnowFlake(SnowFlakeTestBase, DefaultStringFunctionFormulaConnectorTestSuite):
    pass
