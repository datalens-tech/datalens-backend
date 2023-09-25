from dl_connector_snowflake_tests.ext.formula.base import SnowFlakeTestBase  # noqa
from dl_formula_testing.testcases.functions_markup import DefaultMarkupFunctionFormulaConnectorTestSuite


class TestMarkupFunctionSnowFlake(SnowFlakeTestBase, DefaultMarkupFunctionFormulaConnectorTestSuite):
    pass
