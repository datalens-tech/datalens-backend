from dl_formula_testing.testcases.functions_logical import DefaultLogicalFunctionFormulaConnectorTestSuite

from dl_connector_mysql_tests.db.formula.base import (
    MySQL_5_7TestBase,
    MySQL_8_0_12TestBase,
)


class LogicalFunctionMySQLTestSuite(DefaultLogicalFunctionFormulaConnectorTestSuite):
    supports_iif = True


class TestLogicalFunctionMySQL_5_7(MySQL_5_7TestBase, LogicalFunctionMySQLTestSuite):
    pass


class TestLogicalFunctionMySQL_8_0_12(MySQL_8_0_12TestBase, LogicalFunctionMySQLTestSuite):
    supports_any = True
