import pytest

from dl_formula_testing.testcases.functions_markup import DefaultMarkupFunctionFormulaConnectorTestSuite

from dl_connector_starrocks_tests.db.formula.base import StarRocksTestBase


@pytest.mark.skip(reason="Markup function definitions not yet implemented for StarRocks")
class TestMarkupFunctionStarRocks(StarRocksTestBase, DefaultMarkupFunctionFormulaConnectorTestSuite):
    pass
