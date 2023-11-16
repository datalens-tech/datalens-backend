from dl_formula_testing.testcases.functions_string import DefaultStringFunctionFormulaConnectorTestSuite

from dl_connector_ydb_tests.db.formula.base import YQLTestBase


class TestStringFunctionYQL(YQLTestBase, DefaultStringFunctionFormulaConnectorTestSuite):
    datetime_str_separator = "T"
    datetime_str_ending = "Z"
    supports_trimming_funcs = False
    supports_regex_extract = False
    supports_regex_extract_nth = False
    supports_regex_replace = False
    supports_regex_match = False
