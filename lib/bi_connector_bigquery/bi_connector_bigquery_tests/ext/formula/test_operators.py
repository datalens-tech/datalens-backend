from bi_formula.connectors.base.testing.operators import (
    DefaultOperatorFormulaConnectorTestSuite,
)

from bi_connector_bigquery_tests.ext.formula.base import BigQueryTestBase


class TestOperatorBigQuery(BigQueryTestBase, DefaultOperatorFormulaConnectorTestSuite):
    pass
