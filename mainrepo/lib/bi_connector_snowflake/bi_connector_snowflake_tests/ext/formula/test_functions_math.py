from bi_formula_testing.testcases.functions_math import DefaultMathFunctionFormulaConnectorTestSuite

from bi_connector_snowflake_tests.ext.formula.base import SnowFlakeTestBase  # noqa


class TestMathFunctionSnowFlake(SnowFlakeTestBase, DefaultMathFunctionFormulaConnectorTestSuite):
    supports_float_div = True
