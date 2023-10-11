from dl_formula_testing.testcases.functions_datetime import DefaultDateTimeFunctionFormulaConnectorTestSuite

from dl_connector_oracle_tests.db.formula.base import OracleTestBase


class TestDateTimeFunctionOracle(OracleTestBase, DefaultDateTimeFunctionFormulaConnectorTestSuite):
    supports_addition_to_feb_29 = False
    supports_deprecated_dateadd = True
