from dl_formula_testing.testcases.functions_aggregation import DefaultMainAggFunctionFormulaConnectorTestSuite

from dl_connector_postgresql_tests.db.formula.base import (
    PostgreSQL9p3TestBase,
    PostgreSQL9p4TestBase,
)


class MainAggFunctionPostgreTestSuite(DefaultMainAggFunctionFormulaConnectorTestSuite):
    supports_all_concat = True


class TestMainAggFunctionPostgreSQL9p3(PostgreSQL9p3TestBase, MainAggFunctionPostgreTestSuite):
    pass


class TestMainAggFunctionPostgreSQL9p4(PostgreSQL9p4TestBase, MainAggFunctionPostgreTestSuite):
    supports_quantile = True
    supports_median = True
