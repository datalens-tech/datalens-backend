from bi_formula_testing.testcases.literals import (
    DefaultLiteralFormulaConnectorTestSuite,
)
from bi_connector_yql_tests.db.formula.base import (
    YQLTestBase,
)


class TestConditionalBlockYQL(YQLTestBase, DefaultLiteralFormulaConnectorTestSuite):
    supports_microseconds = False
    supports_utc = False
    supports_custom_tz = False
