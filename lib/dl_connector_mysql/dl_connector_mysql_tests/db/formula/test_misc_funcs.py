from dl_formula_testing.testcases.misc_funcs import DefaultMiscFunctionalityConnectorTestSuite

from dl_connector_mysql_tests.db.formula.base import (
    MySQL5p7TestBase,
    MySQL8p0p12TestBase,
)


class TestMiscFunctionalityMySQL5p7(MySQL5p7TestBase, DefaultMiscFunctionalityConnectorTestSuite):
    pass


class TestMiscFunctionalityMySQL8p0p12(MySQL8p0p12TestBase, DefaultMiscFunctionalityConnectorTestSuite):
    pass
