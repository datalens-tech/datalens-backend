from bi_connector_snowflake_tests.ext.formula.base import SnowFlakeTestBase  # noqa
from bi_formula.connectors.base.testing.functions_math import (
    DefaultMathFunctionFormulaConnectorTestSuite,
)


class TestMathFunctionSnowFlake(SnowFlakeTestBase, DefaultMathFunctionFormulaConnectorTestSuite):
    supports_float_div = True
