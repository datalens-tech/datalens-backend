from bi_connector_mssql_tests.db.formula.base import (
    MSSQLTestBase,
)
from bi_formula.connectors.base.testing.misc_funcs import (
    DefaultMiscFunctionalityConnectorTestSuite,
)


class TestMiscFunctionalityMSSQL(MSSQLTestBase, DefaultMiscFunctionalityConnectorTestSuite):
    pass
