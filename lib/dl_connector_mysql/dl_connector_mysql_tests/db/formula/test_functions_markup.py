from dl_formula_testing.testcases.functions_markup import DefaultMarkupFunctionFormulaConnectorTestSuite

from dl_connector_mysql_tests.db.formula.base import (
    MySQL5p7TestBase,
    MySQL8p0p12TestBase,
)


class TestMarkupFunctionMySQL5p7(MySQL5p7TestBase, DefaultMarkupFunctionFormulaConnectorTestSuite):
    pass


class TestMarkupFunctionMySQL8p0p12(MySQL8p0p12TestBase, DefaultMarkupFunctionFormulaConnectorTestSuite):
    pass
