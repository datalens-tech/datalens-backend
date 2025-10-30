from dl_formula_testing.testcases.functions_hash import (
    DefaultHashFunctionFormulaConnectorTestSuite,
    HashFunctionSupport,
)

from dl_connector_clickhouse_tests.db.formula.base import (
    ClickHouse_21_8TestBase,
    ClickHouse_22_10TestBase,
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


class TestHashFunctionClickHouse_21_8(
    ClickHouse_21_8TestBase,
    DefaultHashFunctionFormulaConnectorTestSuite,
):
    hash_function_support = CH_HASH_FUNCTION_SUPPORT


class TestHashFunctionClickHouse_22_10(
    ClickHouse_22_10TestBase,
    DefaultHashFunctionFormulaConnectorTestSuite,
):
    hash_function_support = CH_HASH_FUNCTION_SUPPORT
