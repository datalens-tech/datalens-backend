from dl_formula_testing.testcases.conditional_blocks import DefaultConditionalBlockFormulaConnectorTestSuite

from dl_connector_postgresql_tests.db.formula.base import (
    PostgreSQL9p3TestBase,
    PostgreSQL9p4TestBase,
)


class TestConditionalBlockPostgreSQL9p3(PostgreSQL9p3TestBase, DefaultConditionalBlockFormulaConnectorTestSuite):
    pass


class TestConditionalBlockPostgreSQL9p4(PostgreSQL9p4TestBase, DefaultConditionalBlockFormulaConnectorTestSuite):
    pass
