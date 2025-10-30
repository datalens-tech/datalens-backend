import sqlalchemy as sa

from dl_formula_testing.evaluator import DbEvaluator
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


class HashFunctionClickHouseTestSuite(DefaultHashFunctionFormulaConnectorTestSuite):
    hash_function_support = CH_HASH_FUNCTION_SUPPORT

    def test_murmurhash2_64_multiple_arguments(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval("MurmurHash2_64('DataLens', 1, True)") == 10722267409815771607
        assert dbe.eval("MurmurHash2_64([str_value], 1, True)", from_=data_table) == 16325131390306157643

    def test_siphash64_multiple_arguments(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval("SipHash64('DataLens', 1, True)") == 2343410414584482468
        assert dbe.eval("SipHash64([str_value], 1, True)", from_=data_table) == 9697455610943888398

    def test_cityhash64_multiple_arguments(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert (
            dbe.eval("CityHash64('DataLens', 12345, [str_value], [int_value])", from_=data_table) == 9518705739493127664
        )


class TestHashFunctionClickHouse_21_8(
    ClickHouse_21_8TestBase,
    HashFunctionClickHouseTestSuite,
):
    pass


class TestHashFunctionClickHouse_22_10(
    ClickHouse_22_10TestBase,
    HashFunctionClickHouseTestSuite,
):
    pass
