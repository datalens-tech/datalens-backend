from bi_formula_testing.testcases.functions_aggregation import (
    DefaultMainAggFunctionFormulaConnectorTestSuite,
)

from bi_connector_mysql_tests.db.formula.base import (
    MySQL_5_6TestBase, MySQL_8_0_12TestBase,
)


class TestMainAggFunctionMySQL_5_6(MySQL_5_6TestBase, DefaultMainAggFunctionFormulaConnectorTestSuite):
    pass


class TestMainAggFunctionMySQL_8_0_12(MySQL_8_0_12TestBase, DefaultMainAggFunctionFormulaConnectorTestSuite):
    supports_any = True
