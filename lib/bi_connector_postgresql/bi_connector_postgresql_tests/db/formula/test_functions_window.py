from bi_formula.connectors.base.testing.functions_window import (
    DefaultWindowFunctionFormulaConnectorTestSuite,
)
from bi_connector_postgresql_tests.db.formula.base import (
    PostgreSQL_9_4TestBase, CompengTestBase,
)


class TestWindowFunctionPostgreSQL_9_4(PostgreSQL_9_4TestBase, DefaultWindowFunctionFormulaConnectorTestSuite):
    pass


class TestWindowFunctionCompeng(CompengTestBase, DefaultWindowFunctionFormulaConnectorTestSuite):
    pass
