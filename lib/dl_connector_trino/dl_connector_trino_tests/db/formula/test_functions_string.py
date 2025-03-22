from dl_formula_testing.testcases.functions_string import DefaultStringFunctionFormulaConnectorTestSuite

from dl_connector_trino_tests.db.formula.base import TrinoFormulaTestBase


class TestStringFunctionTrino(TrinoFormulaTestBase, DefaultStringFunctionFormulaConnectorTestSuite):
    supports_regex_extract_all = True
