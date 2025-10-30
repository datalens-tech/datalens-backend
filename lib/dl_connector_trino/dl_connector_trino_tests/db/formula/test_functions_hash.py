from dl_formula_testing.testcases.functions_hash import (
    DefaultHashFunctionFormulaConnectorTestSuite,
    HashFunctionSupport,
)

from dl_connector_trino_tests.db.formula.base import TrinoFormulaTestBase


TRINO_HASH_FUNCTION_SUPPORT = HashFunctionSupport(
    md5=True,
    sha1=True,
    sha256=True,
)


class TestHashFunctionTrino(TrinoFormulaTestBase, DefaultHashFunctionFormulaConnectorTestSuite):
    hash_function_support = TRINO_HASH_FUNCTION_SUPPORT
