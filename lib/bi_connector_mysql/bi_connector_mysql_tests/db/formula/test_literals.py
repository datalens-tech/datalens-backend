from dl_formula_testing.testcases.literals import (
    DefaultLiteralFormulaConnectorTestSuite,
)

from bi_connector_mysql_tests.db.formula.base import (
    MySQL_5_6TestBase, MySQL_8_0_12TestBase,
)


class LogicalFunctionMySQLTestSuite(DefaultLiteralFormulaConnectorTestSuite):
    supports_microseconds = True
    supports_utc = False
    supports_custom_tz = False


class TestLogicalFunctionMySQL_5_6(MySQL_5_6TestBase, LogicalFunctionMySQLTestSuite):
    pass


class TestLogicalFunctionMySQL_8_0_12(MySQL_8_0_12TestBase, LogicalFunctionMySQLTestSuite):
    pass
