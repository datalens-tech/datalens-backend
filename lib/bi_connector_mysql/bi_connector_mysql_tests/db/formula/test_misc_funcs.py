from bi_formula.connectors.base.testing.misc_funcs import (
    DefaultMiscFunctionalityConnectorTestSuite,
)

from bi_connector_mysql_tests.db.formula.base import (
    MySQL_5_6TestBase, MySQL_8_0_12TestBase,
)


class TestMiscFunctionalityMySQL_5_6(MySQL_5_6TestBase, DefaultMiscFunctionalityConnectorTestSuite):
    pass


class TestMiscFunctionalityMySQL_8_0_12(MySQL_8_0_12TestBase, DefaultMiscFunctionalityConnectorTestSuite):
    pass
