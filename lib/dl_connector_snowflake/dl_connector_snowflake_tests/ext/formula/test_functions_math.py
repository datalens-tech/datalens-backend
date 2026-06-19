from dl_formula_testing.testcases.functions_math import DefaultMathFunctionFormulaConnectorTestSuite

from dl_connector_snowflake_tests.ext.formula.base import SnowFlakeTestBase


class TestMathFunctionSnowFlake(SnowFlakeTestBase, DefaultMathFunctionFormulaConnectorTestSuite):
    supports_float_div = True
