from bi_formula_testing.testcases.literals import DefaultLiteralFormulaConnectorTestSuite

from bi_connector_postgresql_tests.db.formula.base import (
    PostgreSQL_9_3TestBase,
    PostgreSQL_9_4TestBase,
)


class ConditionalBlockPostgreSQLTestSuite(DefaultLiteralFormulaConnectorTestSuite):
    supports_microseconds = True
    supports_utc = True
    supports_custom_tz = True


class TestConditionalBlockPostgreSQL_9_3(PostgreSQL_9_3TestBase, ConditionalBlockPostgreSQLTestSuite):
    pass


class TestConditionalBlockPostgreSQL_9_4(PostgreSQL_9_4TestBase, ConditionalBlockPostgreSQLTestSuite):
    pass
