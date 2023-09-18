import pytest

from dl_formula_testing.testcases.functions_string import DefaultStringFunctionFormulaConnectorTestSuite

from bi_connector_mssql_tests.db.formula.base import MSSQLTestBase


class TestStringFunctionMSSQL(MSSQLTestBase, DefaultStringFunctionFormulaConnectorTestSuite):
    datetime_str_ending = " +00:00"
    supports_regex_extract = False
    supports_regex_extract_nth = False
    supports_regex_replace = False
    supports_regex_match = False
    supports_split_3 = False
    supports_non_const_percent_escape = False

    def test_contains_extended(self) -> None:  # type: ignore
        pytest.skip()  # Override base test; default checks not supported
