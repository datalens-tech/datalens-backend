from bi_formula_testing.testcases.functions_math import (
    DefaultMathFunctionFormulaConnectorTestSuite,
)

from bi_connector_mysql_tests.db.formula.base import (
    MySQL_5_6TestBase, MySQL_8_0_12TestBase,
)


class TestMathFunctionMySQL_5_6(MySQL_5_6TestBase, DefaultMathFunctionFormulaConnectorTestSuite):
    pass


class TestMathFunctionMySQL_8_0_12(MySQL_8_0_12TestBase, DefaultMathFunctionFormulaConnectorTestSuite):
    pass
