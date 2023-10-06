from dl_formula_testing.testcases.functions_markup import DefaultMarkupFunctionFormulaConnectorTestSuite

from dl_connector_postgresql_tests.db.formula.base import (
    PostgreSQL_9_3TestBase,
    PostgreSQL_9_4TestBase,
)


class TestMarkupFunctionPostgreSQL_9_3(PostgreSQL_9_3TestBase, DefaultMarkupFunctionFormulaConnectorTestSuite):
    pass


class TestMarkupFunctionPostgreSQL_9_4(PostgreSQL_9_4TestBase, DefaultMarkupFunctionFormulaConnectorTestSuite):
    pass
