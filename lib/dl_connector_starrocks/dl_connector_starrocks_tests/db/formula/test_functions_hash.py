from dl_formula_testing.testcases.functions_hash import (
    DefaultHashFunctionFormulaConnectorTestSuite,
    HashFunctionSupport,
)

from dl_connector_starrocks_tests.db.formula.base import StarRocksTestBase


STARROCKS_HASH_FUNCTION_SUPPORT = HashFunctionSupport(
    md5=True,
    sha1=False,
    sha256=True,
)


class TestHashFunctionStarRocks(StarRocksTestBase, DefaultHashFunctionFormulaConnectorTestSuite):
    hash_function_support = STARROCKS_HASH_FUNCTION_SUPPORT
