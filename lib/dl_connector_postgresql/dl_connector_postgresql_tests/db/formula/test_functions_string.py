from dl_formula_testing.testcases.functions_string import DefaultStringFunctionFormulaConnectorTestSuite

from dl_connector_postgresql_tests.db.formula.base import (
    PostgreSQL_9_3TestBase,
    PostgreSQL_9_4TestBase,
)


class TestStringFunctionPostgreSQL_9_3(PostgreSQL_9_3TestBase, DefaultStringFunctionFormulaConnectorTestSuite):
    supports_regex_extract_all = True


class TestStringFunctionPostgreSQL_9_4(PostgreSQL_9_4TestBase, DefaultStringFunctionFormulaConnectorTestSuite):
    supports_regex_extract_all = True
