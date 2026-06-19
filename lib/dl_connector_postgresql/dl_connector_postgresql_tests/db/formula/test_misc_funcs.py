from dl_formula_testing.testcases.misc_funcs import DefaultMiscFunctionalityConnectorTestSuite

from dl_connector_postgresql_tests.db.formula.base import (
    PostgreSQL9p3TestBase,
    PostgreSQL9p4TestBase,
)


class TestMiscFunctionalityPostgreSQL9p3(PostgreSQL9p3TestBase, DefaultMiscFunctionalityConnectorTestSuite):
    pass


class TestMiscFunctionalityPostgreSQL9p4(PostgreSQL9p4TestBase, DefaultMiscFunctionalityConnectorTestSuite):
    pass
