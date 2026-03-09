import pytest

from dl_formula_testing.testcases.functions_hash import DefaultHashFunctionFormulaConnectorTestSuite

from dl_connector_starrocks_tests.db.formula.base import StarRocksTestBase


@pytest.mark.skip(reason="Hash function definitions not yet implemented for StarRocks")
class TestHashFunctionStarRocks(StarRocksTestBase, DefaultHashFunctionFormulaConnectorTestSuite):
    pass
