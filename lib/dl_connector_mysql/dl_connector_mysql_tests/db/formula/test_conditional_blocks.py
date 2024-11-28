from dl_formula_testing.testcases.conditional_blocks import DefaultConditionalBlockFormulaConnectorTestSuite

from dl_connector_mysql_tests.db.formula.base import (
    MySQL_5_7TestBase,
    MySQL_8_0_40TestBase,
)


class TestLogicalFunctionMySQL_5_7(MySQL_5_7TestBase, DefaultConditionalBlockFormulaConnectorTestSuite):
    pass


class TestLogicalFunctionMySQL_8_0_40(MySQL_8_0_40TestBase, DefaultConditionalBlockFormulaConnectorTestSuite):
    pass
