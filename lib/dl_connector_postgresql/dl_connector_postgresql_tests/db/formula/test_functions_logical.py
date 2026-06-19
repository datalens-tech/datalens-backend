from dl_formula_testing.testcases.functions_logical import DefaultLogicalFunctionFormulaConnectorTestSuite

from dl_connector_postgresql_tests.db.formula.base import (
    PostgreSQL9p3TestBase,
    PostgreSQL9p4TestBase,
)


class LogicalFunctionPostgreTestSuite(DefaultLogicalFunctionFormulaConnectorTestSuite):
    supports_nan_funcs = True
    supports_iif = True


class TestLogicalFunctionPostgreSQL9p3(PostgreSQL9p3TestBase, LogicalFunctionPostgreTestSuite):
    pass


class TestLogicalFunctionPostgreSQL9p4(PostgreSQL9p4TestBase, LogicalFunctionPostgreTestSuite):
    pass
