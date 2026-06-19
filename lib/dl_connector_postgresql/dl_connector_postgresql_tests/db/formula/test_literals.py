from dl_formula_testing.testcases.literals import DefaultLiteralFormulaConnectorTestSuite

from dl_connector_postgresql_tests.db.formula.base import (
    PostgreSQL9p3TestBase,
    PostgreSQL9p4TestBase,
)


class ConditionalBlockPostgreSQLTestSuite(DefaultLiteralFormulaConnectorTestSuite):
    supports_microseconds = True
    supports_utc = True
    supports_custom_tz = True


class TestConditionalBlockPostgreSQL9p3(PostgreSQL9p3TestBase, ConditionalBlockPostgreSQLTestSuite):
    pass


class TestConditionalBlockPostgreSQL9p4(PostgreSQL9p4TestBase, ConditionalBlockPostgreSQLTestSuite):
    pass
