from bi_formula.connectors.base.testing.functions_string import (
    DefaultStringFunctionFormulaConnectorTestSuite,
)

from bi_connector_bigquery_tests.ext.formula.base import (
    BigQueryTestBase,
)


class TestStringFunctionBigQuery(BigQueryTestBase, DefaultStringFunctionFormulaConnectorTestSuite):
    pass
