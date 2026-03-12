from dl_formula_testing.testcases.functions_logical import DefaultLogicalFunctionFormulaConnectorTestSuite

from dl_connector_starrocks_tests.db.formula.base import StarRocksTestBase


class TestLogicalFunctionStarRocks(StarRocksTestBase, DefaultLogicalFunctionFormulaConnectorTestSuite):
    supports_nan_funcs = False
    supports_iif = True
