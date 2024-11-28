from dl_formula_testing.testcases.misc_funcs import DefaultMiscFunctionalityConnectorTestSuite

from dl_connector_mysql_tests.db.formula.base import (
    MySQL_5_7TestBase,
    MySQL_8_0_40TestBase,
)


class TestMiscFunctionalityMySQL_5_7(MySQL_5_7TestBase, DefaultMiscFunctionalityConnectorTestSuite):
    pass


class TestMiscFunctionalityMySQL_8_0_40(MySQL_8_0_40TestBase, DefaultMiscFunctionalityConnectorTestSuite):
    pass
