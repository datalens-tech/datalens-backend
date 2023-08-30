from bi_formula.connectors.base.testing.functions_logical import (
    DefaultLogicalFunctionFormulaConnectorTestSuite,
)
from bi_connector_postgresql_tests.db.formula.base import (
    PostgreSQL_9_3TestBase, PostgreSQL_9_4TestBase,
)


class LogicalFunctionPostgreTestSuite(DefaultLogicalFunctionFormulaConnectorTestSuite):
    supports_nan_funcs = True
    supports_iif = True


class TestLogicalFunctionPostgreSQL_9_3(PostgreSQL_9_3TestBase, LogicalFunctionPostgreTestSuite):
    pass


class TestLogicalFunctionPostgreSQL_9_4(PostgreSQL_9_4TestBase, LogicalFunctionPostgreTestSuite):
    pass
