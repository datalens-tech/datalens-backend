from bi_connector_mssql_tests.db.formula.base import (
    MSSQLTestBase,
)
from bi_formula.connectors.base.testing.functions_markup import (
    DefaultMarkupFunctionFormulaConnectorTestSuite,
)


class TestMarkupFunctionMSSQL(MSSQLTestBase, DefaultMarkupFunctionFormulaConnectorTestSuite):
    pass
