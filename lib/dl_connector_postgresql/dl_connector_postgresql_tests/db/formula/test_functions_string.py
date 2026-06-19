from dl_formula_testing.testcases.functions_string import DefaultStringFunctionFormulaConnectorTestSuite

from dl_connector_postgresql_tests.db.formula.base import (
    PostgreSQL9p3TestBase,
    PostgreSQL9p4TestBase,
)


class TestStringFunctionPostgreSQL9p3(PostgreSQL9p3TestBase, DefaultStringFunctionFormulaConnectorTestSuite):
    supports_regex_extract_all = True


class TestStringFunctionPostgreSQL9p4(PostgreSQL9p4TestBase, DefaultStringFunctionFormulaConnectorTestSuite):
    supports_regex_extract_all = True
