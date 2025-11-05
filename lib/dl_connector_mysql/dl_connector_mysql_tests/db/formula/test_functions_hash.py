from dl_formula_testing.testcases.functions_hash import (
    DefaultHashFunctionFormulaConnectorTestSuite,
    HashFunctionSupport,
)

from dl_connector_mysql_tests.db.formula.base import (
    MySQL_5_7TestBase,
    MySQL_8_0_12TestBase,
)


MYSQL_HASH_FUNCTION_SUPPORT = HashFunctionSupport(
    md5=True,
    sha1=True,
    sha256=True,
)


class HashFunctionMySQLTestSuite(DefaultHashFunctionFormulaConnectorTestSuite):
    hash_function_support = MYSQL_HASH_FUNCTION_SUPPORT


class TestHashFunctionMySQL_5_7(
    MySQL_5_7TestBase,
    HashFunctionMySQLTestSuite,
):
    pass


class TestHashFunctionMySQL_8_0_12(
    MySQL_8_0_12TestBase,
    HashFunctionMySQLTestSuite,
):
    pass
