from dl_formula_testing.testcases.functions_hash import (
    DefaultHashFunctionFormulaConnectorTestSuite,
    HashFunctionSupport,
)

from dl_connector_mysql_tests.db.formula.base import (
    MySQL5p7TestBase,
    MySQL8p0p12TestBase,
)

MYSQL_HASH_FUNCTION_SUPPORT = HashFunctionSupport(
    md5=True,
    sha1=True,
    sha256=True,
)


class HashFunctionMySQLTestSuite(DefaultHashFunctionFormulaConnectorTestSuite):
    hash_function_support = MYSQL_HASH_FUNCTION_SUPPORT


class TestHashFunctionMySQL5p7(
    MySQL5p7TestBase,
    HashFunctionMySQLTestSuite,
):
    pass


class TestHashFunctionMySQL8p0p12(
    MySQL8p0p12TestBase,
    HashFunctionMySQLTestSuite,
):
    pass
