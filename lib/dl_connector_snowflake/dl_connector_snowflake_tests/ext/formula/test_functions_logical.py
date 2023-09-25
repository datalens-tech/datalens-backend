from dl_connector_snowflake_tests.ext.formula.base import SnowFlakeTestBase  # noqa
from dl_formula_testing.testcases.functions_logical import DefaultLogicalFunctionFormulaConnectorTestSuite


class TestLogicalFunctionSnowFlake(SnowFlakeTestBase, DefaultLogicalFunctionFormulaConnectorTestSuite):
    supports_nan_funcs = False
    supports_iif = False
