from bi_connector_snowflake_tests.ext.formula.base import SnowFlakeTestBase  # noqa
from bi_formula.connectors.base.testing.functions_string import (
    DefaultStringFunctionFormulaConnectorTestSuite,
)


class TestStringFunctionSnowFlake(SnowFlakeTestBase, DefaultStringFunctionFormulaConnectorTestSuite):
    pass
