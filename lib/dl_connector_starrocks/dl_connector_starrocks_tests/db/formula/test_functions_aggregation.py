from dl_formula_testing.testcases.functions_aggregation import DefaultMainAggFunctionFormulaConnectorTestSuite

from dl_connector_starrocks_tests.db.formula.base import StarRocksTestBase


class TestAggFunctionsStarRocks(StarRocksTestBase, DefaultMainAggFunctionFormulaConnectorTestSuite):  # type: ignore  # 2024-01-30 # TODO: fix
    supports_all_concat = False  # MVP doesn't include ALL_CONCAT
    supports_quantile = False
    supports_median = False
