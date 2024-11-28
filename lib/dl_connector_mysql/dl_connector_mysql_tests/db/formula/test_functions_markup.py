from dl_formula_testing.testcases.functions_markup import DefaultMarkupFunctionFormulaConnectorTestSuite

from dl_connector_mysql_tests.db.formula.base import (
    MySQL_5_7TestBase,
    MySQL_8_0_40TestBase,
)


class TestMarkupFunctionMySQL_5_7(MySQL_5_7TestBase, DefaultMarkupFunctionFormulaConnectorTestSuite):
    pass


class TestMarkupFunctionMySQL_8_0_40(MySQL_8_0_40TestBase, DefaultMarkupFunctionFormulaConnectorTestSuite):
    pass
