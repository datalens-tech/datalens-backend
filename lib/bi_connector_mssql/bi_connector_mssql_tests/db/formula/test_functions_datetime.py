from bi_connector_mssql_tests.db.formula.base import (
    MSSQLTestBase,
)
from dl_formula_testing.testcases.functions_datetime import (
    DefaultDateTimeFunctionFormulaConnectorTestSuite,
)


class TestDateTimeFunctionMSSQL(MSSQLTestBase, DefaultDateTimeFunctionFormulaConnectorTestSuite):
    supports_deprecated_dateadd = True
    supports_deprecated_datepart_2 = True
