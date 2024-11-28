from dl_formula_testing.testcases.literals import DefaultLiteralFormulaConnectorTestSuite

from dl_connector_mysql_tests.db.formula.base import (
    MySQL_5_7TestBase,
    MySQL_8_0_40TestBase,
)


class LogicalFunctionMySQLTestSuite(DefaultLiteralFormulaConnectorTestSuite):
    supports_microseconds = True
    supports_utc = False
    supports_custom_tz = False


class TestLogicalFunctionMySQL_5_7(MySQL_5_7TestBase, LogicalFunctionMySQLTestSuite):
    pass


class TestLogicalFunctionMySQL_8_0_40(MySQL_8_0_40TestBase, LogicalFunctionMySQLTestSuite):
    pass
