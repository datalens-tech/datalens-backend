from dl_formula_testing.testcases.literals import DefaultLiteralFormulaConnectorTestSuite

from dl_connector_trino_tests.db.formula.base import TrinoFormulaTestBase


class TestLiteralTrino(TrinoFormulaTestBase, DefaultLiteralFormulaConnectorTestSuite):
    supports_microseconds = True
    supports_utc = True
    supports_custom_tz = True
