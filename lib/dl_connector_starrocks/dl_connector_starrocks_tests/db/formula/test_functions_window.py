import pytest

from dl_formula_testing.testcases.functions_window import DefaultWindowFunctionFormulaConnectorTestSuite

from dl_connector_starrocks_tests.db.formula.base import StarRocksTestBase


@pytest.mark.skip(reason="Window function definitions not yet implemented for StarRocks")
class TestWindowFunctionStarRocks(StarRocksTestBase, DefaultWindowFunctionFormulaConnectorTestSuite):
    pass
