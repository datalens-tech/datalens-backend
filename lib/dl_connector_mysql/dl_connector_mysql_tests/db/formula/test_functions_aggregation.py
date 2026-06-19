from dl_formula_testing.testcases.functions_aggregation import DefaultMainAggFunctionFormulaConnectorTestSuite

from dl_connector_mysql_tests.db.formula.base import (
    MySQL5p7TestBase,
    MySQL8p0p12TestBase,
)


class TestMainAggFunctionMySQL5p7(MySQL5p7TestBase, DefaultMainAggFunctionFormulaConnectorTestSuite):
    pass


class TestMainAggFunctionMySQL8p0p12(MySQL8p0p12TestBase, DefaultMainAggFunctionFormulaConnectorTestSuite):
    supports_any = True
