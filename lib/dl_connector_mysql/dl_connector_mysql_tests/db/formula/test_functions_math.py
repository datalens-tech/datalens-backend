from dl_formula_testing.testcases.functions_math import DefaultMathFunctionFormulaConnectorTestSuite

from dl_connector_mysql_tests.db.formula.base import (
    MySQL5p7TestBase,
    MySQL8p0p12TestBase,
)


class TestMathFunctionMySQL5p7(MySQL5p7TestBase, DefaultMathFunctionFormulaConnectorTestSuite):
    pass


class TestMathFunctionMySQL8p0p12(MySQL8p0p12TestBase, DefaultMathFunctionFormulaConnectorTestSuite):
    pass
