from dl_formula_testing.testcases.functions_datetime import DefaultDateTimeFunctionFormulaConnectorTestSuite

from dl_connector_mysql_tests.db.formula.base import (
    MySQL5p7TestBase,
    MySQL8p0p12TestBase,
)


class DateTimeFunctionMySQLTestSuite(DefaultDateTimeFunctionFormulaConnectorTestSuite):
    supports_deprecated_dateadd = True
    supports_deprecated_datepart_2 = True


class TestDateTimeFunctionMySQL5p7(MySQL5p7TestBase, DateTimeFunctionMySQLTestSuite):
    pass


class TestDateTimeFunctionMySQL8p0p12(MySQL8p0p12TestBase, DateTimeFunctionMySQLTestSuite):
    pass
