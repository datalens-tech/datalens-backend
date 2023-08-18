from bi_formula.connectors.base.testing.misc_funcs import (
    DefaultMiscFunctionalityConnectorTestSuite,
)
from bi_connector_postgresql_tests.db.formula.base import (
    PostgreSQL_9_3TestBase, PostgreSQL_9_4TestBase,
)


class TestMiscFunctionalityPostgreSQL_9_3(PostgreSQL_9_3TestBase, DefaultMiscFunctionalityConnectorTestSuite):
    pass


class TestMiscFunctionalityPostgreSQL_9_4(PostgreSQL_9_4TestBase, DefaultMiscFunctionalityConnectorTestSuite):
    pass
