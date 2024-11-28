from dl_formula_testing.testcases.operators import DefaultOperatorFormulaConnectorTestSuite

from dl_connector_mysql_tests.db.formula.base import (
    MySQL_5_7TestBase,
    MySQL_8_0_40TestBase,
)


class OperatorMySQLTestSuite(DefaultOperatorFormulaConnectorTestSuite):
    subtraction_round_dt = False


class TestOperatorMySQL_5_7(MySQL_5_7TestBase, OperatorMySQLTestSuite):
    pass


class TestOperatorMySQL_8_0_40(MySQL_8_0_40TestBase, OperatorMySQLTestSuite):
    pass
