from bi_connector_mssql_tests.db.formula.base import (
    MSSQLTestBase,
)
from bi_formula_testing.testcases.functions_aggregation import (
    DefaultMainAggFunctionFormulaConnectorTestSuite,
)


class TestMainAggFunctionMSSQL(MSSQLTestBase, DefaultMainAggFunctionFormulaConnectorTestSuite):
    pass
