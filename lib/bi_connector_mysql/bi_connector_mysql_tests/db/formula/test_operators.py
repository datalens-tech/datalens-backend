from dl_formula_testing.testcases.operators import (
    DefaultOperatorFormulaConnectorTestSuite,
)

from bi_connector_mysql_tests.db.formula.base import (
    MySQL_5_6TestBase, MySQL_8_0_12TestBase,
)


class OperatorMySQLTestSuite(DefaultOperatorFormulaConnectorTestSuite):
    subtraction_round_dt = False


class TestOperatorMySQL_5_6(MySQL_5_6TestBase, OperatorMySQLTestSuite):
    pass


class TestOperatorMySQL_8_0_12(MySQL_8_0_12TestBase, OperatorMySQLTestSuite):
    pass
