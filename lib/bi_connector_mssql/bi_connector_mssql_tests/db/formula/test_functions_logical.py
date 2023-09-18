from bi_connector_mssql_tests.db.formula.base import (
    MSSQLTestBase,
)
from dl_formula_testing.testcases.functions_logical import (
    DefaultLogicalFunctionFormulaConnectorTestSuite,
)


class TestLogicalFunctionMSSQL(MSSQLTestBase, DefaultLogicalFunctionFormulaConnectorTestSuite):
    supports_iif = True
