from bi_connector_mssql_tests.db.formula.base import (
    MSSQLTestBase,
)
from dl_formula_testing.testcases.functions_markup import (
    DefaultMarkupFunctionFormulaConnectorTestSuite,
)


class TestMarkupFunctionMSSQL(MSSQLTestBase, DefaultMarkupFunctionFormulaConnectorTestSuite):
    pass
