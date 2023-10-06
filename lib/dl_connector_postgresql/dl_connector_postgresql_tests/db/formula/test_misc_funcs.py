from dl_formula_testing.testcases.misc_funcs import DefaultMiscFunctionalityConnectorTestSuite

from dl_connector_postgresql_tests.db.formula.base import (
    PostgreSQL_9_3TestBase,
    PostgreSQL_9_4TestBase,
)


class TestMiscFunctionalityPostgreSQL_9_3(PostgreSQL_9_3TestBase, DefaultMiscFunctionalityConnectorTestSuite):
    pass


class TestMiscFunctionalityPostgreSQL_9_4(PostgreSQL_9_4TestBase, DefaultMiscFunctionalityConnectorTestSuite):
    pass
