from dl_formula_testing.testcases.operators import DefaultOperatorFormulaConnectorTestSuite

from dl_connector_postgresql_tests.db.formula.base import (
    PostgreSQL9p3TestBase,
    PostgreSQL9p4TestBase,
)


class OperatorPostgreSQLTestSuite(DefaultOperatorFormulaConnectorTestSuite):
    subtraction_round_dt = False
    make_float_array_cast = "double precision[]"
    make_str_array_cast = "text[]"


class TestOperatorPostgreSQL9p3(PostgreSQL9p3TestBase, OperatorPostgreSQLTestSuite):
    pass


class TestOperatorPostgreSQL9p4(PostgreSQL9p4TestBase, OperatorPostgreSQLTestSuite):
    pass
