from dl_formula_testing.testcases.functions_logical import DefaultLogicalFunctionFormulaConnectorTestSuite

from dl_connector_oracle_tests.db.formula.base import OracleTestBase


class TestLogicalFunctionOracle(OracleTestBase, DefaultLogicalFunctionFormulaConnectorTestSuite):
    supports_iif = True
