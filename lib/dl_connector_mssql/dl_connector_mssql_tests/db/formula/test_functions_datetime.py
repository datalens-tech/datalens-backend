from dl_formula_testing.testcases.functions_datetime import DefaultDateTimeFunctionFormulaConnectorTestSuite

from dl_connector_mssql_tests.db.formula.base import MSSQLTestBase


class TestDateTimeFunctionMSSQL(MSSQLTestBase, DefaultDateTimeFunctionFormulaConnectorTestSuite):
    supports_deprecated_dateadd = True
    supports_deprecated_datepart_2 = True
