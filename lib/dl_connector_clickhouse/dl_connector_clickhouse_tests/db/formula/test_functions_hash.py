from dl_formula_testing.testcases.functions_hash import (
    DefaultHashFunctionFormulaConnectorTestSuite,
    HashFunctionSupport,
)

from dl_connector_clickhouse_tests.db.formula.base import (
    ClickHouse21p8TestBase,
    ClickHouse22p10TestBase,
)

CH_HASH_FUNCTION_SUPPORT = HashFunctionSupport(
    md5=True,
    sha1=True,
    sha256=True,
    murmurhash2_64=True,
    siphash64=True,
    inthash64=True,
    cityhash64=True,
)


class HashFunctionClickHouseTestSuite(DefaultHashFunctionFormulaConnectorTestSuite):
    hash_function_support = CH_HASH_FUNCTION_SUPPORT


class TestHashFunctionClickHouse21p8(
    ClickHouse21p8TestBase,
    HashFunctionClickHouseTestSuite,
):
    pass


class TestHashFunctionClickHouse22p10(
    ClickHouse22p10TestBase,
    HashFunctionClickHouseTestSuite,
):
    pass
