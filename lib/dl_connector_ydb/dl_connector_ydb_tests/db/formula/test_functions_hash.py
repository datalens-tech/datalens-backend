from dl_formula_testing.testcases.functions_hash import (
    DefaultHashFunctionFormulaConnectorTestSuite,
    HashFunctionSupport,
)

from dl_connector_ydb_tests.db.formula.base import YQLTestBase


YDB_HASH_FUNCTION_SUPPORT = HashFunctionSupport(
    md5=True,
    sha1=True,
    sha256=True,
    murmurhash2_64=True,
    siphash64=True,
    inthash64=True,
    cityhash64=True,
)


class TestHashFunctionYDB(YQLTestBase, DefaultHashFunctionFormulaConnectorTestSuite):
    hash_function_support = YDB_HASH_FUNCTION_SUPPORT
