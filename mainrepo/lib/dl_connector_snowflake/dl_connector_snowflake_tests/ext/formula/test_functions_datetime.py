from dl_formula_testing.testcases.functions_datetime import DefaultDateTimeFunctionFormulaConnectorTestSuite

from dl_connector_snowflake_tests.ext.formula.base import SnowFlakeTestBase  # noqa


class TestDateTimeFunctionSnowFlake(SnowFlakeTestBase, DefaultDateTimeFunctionFormulaConnectorTestSuite):
    supports_addition_to_feb_29 = True
    supports_deprecated_dateadd = True
