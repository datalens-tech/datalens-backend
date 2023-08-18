from bi_formula.connectors.base.testing.functions_datetime import (
    DefaultDateTimeFunctionFormulaConnectorTestSuite,
)
from bi_connector_yql_tests.db.formula.base import (
    YQLTestBase,
)


class TestDateTimeFunctionYQL(YQLTestBase, DefaultDateTimeFunctionFormulaConnectorTestSuite):
    supports_datepart_2_non_const = False
