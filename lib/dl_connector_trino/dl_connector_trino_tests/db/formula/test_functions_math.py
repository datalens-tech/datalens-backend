from dl_formula_testing.testcases.functions_math import DefaultMathFunctionFormulaConnectorTestSuite

from dl_connector_trino_tests.db.formula.base import TrinoFormulaTestBase


class TestMathFunctionTrino(
    TrinoFormulaTestBase,
    DefaultMathFunctionFormulaConnectorTestSuite,
):
    pass
