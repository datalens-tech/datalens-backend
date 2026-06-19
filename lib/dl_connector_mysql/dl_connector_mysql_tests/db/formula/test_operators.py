from dl_formula_testing.testcases.operators import DefaultOperatorFormulaConnectorTestSuite

from dl_connector_mysql_tests.db.formula.base import (
    MySQL5p7TestBase,
    MySQL8p0p12TestBase,
)


class OperatorMySQLTestSuite(DefaultOperatorFormulaConnectorTestSuite):
    subtraction_round_dt = False


class TestOperatorMySQL5p7(MySQL5p7TestBase, OperatorMySQLTestSuite):
    pass


class TestOperatorMySQL8p0p12(MySQL8p0p12TestBase, OperatorMySQLTestSuite):
    pass
