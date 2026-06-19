from dl_formula_testing.testcases.functions_window import DefaultWindowFunctionFormulaConnectorTestSuite

from dl_connector_postgresql_tests.db.formula.base import (
    CompengTestBase,
    PostgreSQL9p4TestBase,
)


class TestWindowFunctionPostgreSQL9p4(PostgreSQL9p4TestBase, DefaultWindowFunctionFormulaConnectorTestSuite):
    pass


class TestWindowFunctionCompeng(CompengTestBase, DefaultWindowFunctionFormulaConnectorTestSuite):
    pass
