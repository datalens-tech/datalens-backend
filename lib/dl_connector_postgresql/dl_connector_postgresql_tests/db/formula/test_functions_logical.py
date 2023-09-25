from dl_connector_postgresql_tests.db.formula.base import (
    PostgreSQL_9_3TestBase,
    PostgreSQL_9_4TestBase,
)
from dl_formula_testing.testcases.functions_logical import DefaultLogicalFunctionFormulaConnectorTestSuite


class LogicalFunctionPostgreTestSuite(DefaultLogicalFunctionFormulaConnectorTestSuite):
    supports_nan_funcs = True
    supports_iif = True


class TestLogicalFunctionPostgreSQL_9_3(PostgreSQL_9_3TestBase, LogicalFunctionPostgreTestSuite):
    pass


class TestLogicalFunctionPostgreSQL_9_4(PostgreSQL_9_4TestBase, LogicalFunctionPostgreTestSuite):
    pass
