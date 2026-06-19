from dl_formula_testing.testcases.functions_markup import DefaultMarkupFunctionFormulaConnectorTestSuite

from dl_connector_postgresql_tests.db.formula.base import (
    PostgreSQL9p3TestBase,
    PostgreSQL9p4TestBase,
)


class TestMarkupFunctionPostgreSQL9p3(PostgreSQL9p3TestBase, DefaultMarkupFunctionFormulaConnectorTestSuite):
    pass


class TestMarkupFunctionPostgreSQL9p4(PostgreSQL9p4TestBase, DefaultMarkupFunctionFormulaConnectorTestSuite):
    pass
