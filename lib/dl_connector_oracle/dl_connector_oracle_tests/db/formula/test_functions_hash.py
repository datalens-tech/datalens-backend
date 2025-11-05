from dl_formula_testing.testcases.functions_hash import (
    DefaultHashFunctionFormulaConnectorTestSuite,
    HashFunctionSupport,
)

from dl_connector_oracle_tests.db.formula.base import OracleTestBase


ORACLE_HASH_FUNCTION_SUPPORT = HashFunctionSupport(
    md5=True,
    sha1=True,
    sha256=True,
)


class HashFunctionOracleTestSuite(DefaultHashFunctionFormulaConnectorTestSuite):
    hash_function_support = ORACLE_HASH_FUNCTION_SUPPORT


class TestHashFunctionOracle(
    OracleTestBase,
    HashFunctionOracleTestSuite,
):
    pass
