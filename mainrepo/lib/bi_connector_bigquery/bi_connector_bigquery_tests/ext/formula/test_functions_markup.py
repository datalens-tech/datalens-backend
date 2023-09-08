from bi_formula_testing.testcases.functions_markup import (
    DefaultMarkupFunctionFormulaConnectorTestSuite,
)

from bi_connector_bigquery_tests.ext.formula.base import BigQueryTestBase


class TestMarkupFunctionBigQuery(BigQueryTestBase, DefaultMarkupFunctionFormulaConnectorTestSuite):
    pass
