from dl_formula_testing.testcases.conditional_blocks import DefaultConditionalBlockFormulaConnectorTestSuite

from dl_connector_mysql_tests.db.formula.base import (
    MySQL5p7TestBase,
    MySQL8p0p12TestBase,
)


class TestLogicalFunctionMySQL5p7(MySQL5p7TestBase, DefaultConditionalBlockFormulaConnectorTestSuite):
    pass


class TestLogicalFunctionMySQL8p0p12(MySQL8p0p12TestBase, DefaultConditionalBlockFormulaConnectorTestSuite):
    pass
