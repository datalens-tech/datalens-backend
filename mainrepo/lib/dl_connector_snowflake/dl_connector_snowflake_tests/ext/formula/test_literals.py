from dl_connector_snowflake_tests.ext.formula.base import SnowFlakeTestBase  # noqa
from dl_formula_testing.testcases.literals import DefaultLiteralFormulaConnectorTestSuite


class TestLiteralSnowFlake(SnowFlakeTestBase, DefaultLiteralFormulaConnectorTestSuite):
    supports_microseconds = True
    supports_utc = False
    supports_custom_tz = False
