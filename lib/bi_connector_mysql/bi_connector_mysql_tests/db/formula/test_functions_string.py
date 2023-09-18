from dl_formula_testing.testcases.functions_string import (
    DefaultStringFunctionFormulaConnectorTestSuite,
)

from bi_connector_mysql_tests.db.formula.base import (
    MySQL_5_6TestBase, MySQL_8_0_12TestBase,
)


class StringFunctionMySQLTestSuite(DefaultStringFunctionFormulaConnectorTestSuite):
    datetime_str_ending = '.000000'


class TestStringFunctionMySQL_5_6(MySQL_5_6TestBase, StringFunctionMySQLTestSuite):
    supports_regex_extract = False
    supports_regex_extract_nth = False
    supports_regex_replace = False


class TestStringFunctionMySQL_8_0_12(MySQL_8_0_12TestBase, StringFunctionMySQLTestSuite):
    pass
