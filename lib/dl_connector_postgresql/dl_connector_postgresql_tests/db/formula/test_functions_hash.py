from dl_formula_testing.testcases.functions_hash import (
    DefaultHashFunctionFormulaConnectorTestSuite,
    HashFunctionSupport,
)

from dl_connector_postgresql_tests.db.formula.base import (
    PostgreSQL_9_3TestBase,
    PostgreSQL_9_4TestBase,
)


PG_HASH_FUNCTION_SUPPORT = HashFunctionSupport(
    md5=True,
    sha1=True,
    sha256=True,
)


class TestHashFunctionPostgreSQL_9_3(
    PostgreSQL_9_3TestBase,
    DefaultHashFunctionFormulaConnectorTestSuite,
):
    hash_function_support = PG_HASH_FUNCTION_SUPPORT


class TestHashFunctionPostgreSQL_9_4(
    PostgreSQL_9_4TestBase,
    DefaultHashFunctionFormulaConnectorTestSuite,
):
    hash_function_support = PG_HASH_FUNCTION_SUPPORT
