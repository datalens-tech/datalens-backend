from bi_formula.connectors.base.testing.functions_datetime import (
    DefaultDateTimeFunctionFormulaConnectorTestSuite,
)

from bi_connector_bigquery_tests.ext.formula.base import BigQueryTestBase


class TestDateTimeFunctionBigQuery(BigQueryTestBase, DefaultDateTimeFunctionFormulaConnectorTestSuite):
    pass
