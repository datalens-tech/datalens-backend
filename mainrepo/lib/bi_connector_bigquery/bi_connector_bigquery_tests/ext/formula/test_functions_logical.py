from bi_formula.connectors.base.testing.functions_logical import (
    DefaultLogicalFunctionFormulaConnectorTestSuite,
)

from bi_connector_bigquery_tests.ext.formula.base import BigQueryTestBase


class TestLogicalFunctionBigQuery(BigQueryTestBase, DefaultLogicalFunctionFormulaConnectorTestSuite):
    pass
