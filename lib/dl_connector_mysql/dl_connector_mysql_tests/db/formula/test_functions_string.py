from dl_formula_testing.testcases.functions_string import DefaultStringFunctionFormulaConnectorTestSuite

from dl_connector_mysql_tests.db.formula.base import (
    MySQL5p7TestBase,
    MySQL8p0p12TestBase,
)


class StringFunctionMySQLTestSuite(DefaultStringFunctionFormulaConnectorTestSuite):
    datetime_str_ending = ".000000"


class TestStringFunctionMySQL5p7(MySQL5p7TestBase, StringFunctionMySQLTestSuite):
    supports_regex_extract = False
    supports_regex_extract_nth = False
    supports_regex_replace = False


class TestStringFunctionMySQL8p0p12(MySQL8p0p12TestBase, StringFunctionMySQLTestSuite):
    pass
