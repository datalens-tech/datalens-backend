from bi_formula.connectors.base.testing.functions_markup import (
    DefaultMarkupFunctionFormulaConnectorTestSuite,
)

from bi_connector_mysql_tests.db.formula.base import (
    MySQL_5_6TestBase, MySQL_8_0_12TestBase,
)


class TestMarkupFunctionMySQL_5_6(MySQL_5_6TestBase, DefaultMarkupFunctionFormulaConnectorTestSuite):
    pass


class TestMarkupFunctionMySQL_8_0_12(MySQL_8_0_12TestBase, DefaultMarkupFunctionFormulaConnectorTestSuite):
    pass
