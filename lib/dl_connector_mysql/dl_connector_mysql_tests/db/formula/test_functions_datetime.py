from dl_formula_testing.testcases.functions_datetime import DefaultDateTimeFunctionFormulaConnectorTestSuite

from dl_connector_mysql_tests.db.formula.base import (
    MySQL_5_7TestBase,
    MySQL_8_0_12TestBase,
)


class DateTimeFunctionMySQLTestSuite(DefaultDateTimeFunctionFormulaConnectorTestSuite):
    supports_deprecated_dateadd = True
    supports_deprecated_datepart_2 = True


class TestDateTimeFunctionMySQL_5_7(MySQL_5_7TestBase, DateTimeFunctionMySQLTestSuite):
    pass


class TestDateTimeFunctionMySQL_8_0_12(MySQL_8_0_12TestBase, DateTimeFunctionMySQLTestSuite):
    pass
