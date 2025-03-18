from dl_formula_testing.testcases.operators import DefaultOperatorFormulaConnectorTestSuite

from dl_connector_trino_tests.db.formula.base import TrinoFormulaTestBase


class TestOperatorTrino(TrinoFormulaTestBase, DefaultOperatorFormulaConnectorTestSuite):
    subtraction_round_dt = False
