from dl_formula_testing.testcases.functions_math import DefaultMathFunctionFormulaConnectorTestSuite

from dl_connector_mysql_tests.db.formula.base import (
    MySQL_5_7TestBase,
    MySQL_8_0_12TestBase,
)


class TestMathFunctionMySQL_5_7(MySQL_5_7TestBase, DefaultMathFunctionFormulaConnectorTestSuite):
    pass


class TestMathFunctionMySQL_8_0_12(MySQL_8_0_12TestBase, DefaultMathFunctionFormulaConnectorTestSuite):
    pass
