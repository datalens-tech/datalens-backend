from dl_connector_postgresql_tests.db.formula.base import (
    CompengTestBase,
    PostgreSQL_9_4TestBase,
)
from dl_formula_testing.testcases.functions_window import DefaultWindowFunctionFormulaConnectorTestSuite


class TestWindowFunctionPostgreSQL_9_4(PostgreSQL_9_4TestBase, DefaultWindowFunctionFormulaConnectorTestSuite):
    pass


class TestWindowFunctionCompeng(CompengTestBase, DefaultWindowFunctionFormulaConnectorTestSuite):
    pass
