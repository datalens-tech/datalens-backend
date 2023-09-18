from dl_formula_testing.testcases.functions_window import (
    DefaultWindowFunctionFormulaConnectorTestSuite,
)
from bi_connector_mysql_tests.db.formula.base import (
    MySQL_8_0_12TestBase,
)


class TestWindowFunctionMySQL_8_0_12(MySQL_8_0_12TestBase, DefaultWindowFunctionFormulaConnectorTestSuite):
    pass
