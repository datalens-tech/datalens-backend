from bi_formula_testing.testcases.functions_math import (
    DefaultMathFunctionFormulaConnectorTestSuite,
)

from bi_connector_bigquery_tests.ext.formula.base import BigQueryTestBase


class TestMathFunctionBigQuery(BigQueryTestBase, DefaultMathFunctionFormulaConnectorTestSuite):
    supports_float_div = False
