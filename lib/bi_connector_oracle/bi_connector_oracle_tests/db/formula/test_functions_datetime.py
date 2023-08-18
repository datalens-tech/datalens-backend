from bi_formula.connectors.base.testing.functions_datetime import (
    DefaultDateTimeFunctionFormulaConnectorTestSuite,
)

from bi_connector_oracle_tests.db.formula.base import (
    OracleTestBase,
)


class TestDateTimeFunctionOracle(OracleTestBase, DefaultDateTimeFunctionFormulaConnectorTestSuite):
    supports_addition_to_feb_29 = False
    supports_deprecated_dateadd = True
