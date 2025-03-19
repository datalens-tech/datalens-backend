from dl_formula_testing.testcases.functions_logical import DefaultLogicalFunctionFormulaConnectorTestSuite

from dl_connector_trino_tests.db.formula.base import TrinoFormulaTestBase


class TestLogicalFunctionTrino(TrinoFormulaTestBase, DefaultLogicalFunctionFormulaConnectorTestSuite):
    supports_nan_funcs = True
    supports_iif = True
