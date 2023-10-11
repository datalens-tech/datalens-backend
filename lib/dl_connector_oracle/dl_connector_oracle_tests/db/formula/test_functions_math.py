from dl_formula_testing.testcases.functions_math import DefaultMathFunctionFormulaConnectorTestSuite

from dl_connector_oracle_tests.db.formula.base import OracleTestBase


class TestMathFunctionOracle(OracleTestBase, DefaultMathFunctionFormulaConnectorTestSuite):
    supports_atan_2_in_origin = False
