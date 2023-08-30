from bi_formula.connectors.base.testing.functions_aggregation import (
    DefaultMainAggFunctionFormulaConnectorTestSuite,
)
from bi_connector_postgresql_tests.db.formula.base import (
    PostgreSQL_9_3TestBase, PostgreSQL_9_4TestBase,
)


class MainAggFunctionPostgreTestSuite(DefaultMainAggFunctionFormulaConnectorTestSuite):
    supports_all_concat = True


class TestMainAggFunctionPostgreSQL_9_3(PostgreSQL_9_3TestBase, MainAggFunctionPostgreTestSuite):
    pass


class TestMainAggFunctionPostgreSQL_9_4(PostgreSQL_9_4TestBase, MainAggFunctionPostgreTestSuite):
    supports_quantile = True
    supports_median = True
