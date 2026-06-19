from dl_formula_testing.testcases.literals import DefaultLiteralFormulaConnectorTestSuite

from dl_connector_mysql_tests.db.formula.base import (
    MySQL5p7TestBase,
    MySQL8p0p12TestBase,
)


class LogicalFunctionMySQLTestSuite(DefaultLiteralFormulaConnectorTestSuite):
    supports_microseconds = True
    supports_utc = False
    supports_custom_tz = False


class TestLogicalFunctionMySQL5p7(MySQL5p7TestBase, LogicalFunctionMySQLTestSuite):
    pass


class TestLogicalFunctionMySQL8p0p12(MySQL8p0p12TestBase, LogicalFunctionMySQLTestSuite):
    pass
