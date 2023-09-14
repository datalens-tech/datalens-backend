from bi_formula_testing.testcases.conditional_blocks import DefaultConditionalBlockFormulaConnectorTestSuite

from bi_connector_postgresql_tests.db.formula.base import (
    PostgreSQL_9_3TestBase,
    PostgreSQL_9_4TestBase,
)


class TestConditionalBlockPostgreSQL_9_3(PostgreSQL_9_3TestBase, DefaultConditionalBlockFormulaConnectorTestSuite):
    pass


class TestConditionalBlockPostgreSQL_9_4(PostgreSQL_9_4TestBase, DefaultConditionalBlockFormulaConnectorTestSuite):
    pass
