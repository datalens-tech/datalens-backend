from dl_formula_testing.testcases.functions_logical import DefaultLogicalFunctionFormulaConnectorTestSuite

from dl_connector_mysql_tests.db.formula.base import (
    MySQL5p7TestBase,
    MySQL8p0p12TestBase,
)


class LogicalFunctionMySQLTestSuite(DefaultLogicalFunctionFormulaConnectorTestSuite):
    supports_iif = True


class TestLogicalFunctionMySQL5p7(MySQL5p7TestBase, LogicalFunctionMySQLTestSuite):
    pass


class TestLogicalFunctionMySQL8p0p12(MySQL8p0p12TestBase, LogicalFunctionMySQLTestSuite):
    supports_any = True
