from dl_connector_postgresql_tests.db.formula.base import (
    PostgreSQL_9_3TestBase,
    PostgreSQL_9_4TestBase,
)
from dl_formula_testing.testcases.operators import DefaultOperatorFormulaConnectorTestSuite


class OperatorPostgreSQLTestSuite(DefaultOperatorFormulaConnectorTestSuite):
    subtraction_round_dt = False
    make_float_array_cast = "double precision[]"


class TestOperatorPostgreSQL_9_3(PostgreSQL_9_3TestBase, OperatorPostgreSQLTestSuite):
    pass


class TestOperatorPostgreSQL_9_4(PostgreSQL_9_4TestBase, OperatorPostgreSQLTestSuite):
    pass
