from dl_formula_testing.testcases.literals import DefaultLiteralFormulaConnectorTestSuite

from dl_connector_ydb_tests.db.formula.base import YQLTestBase


class TestConditionalBlockYQL(YQLTestBase, DefaultLiteralFormulaConnectorTestSuite):
    supports_microseconds = False
    supports_utc = False
    supports_custom_tz = False

    def test_number123(self, dbe) -> None:  # TODO remove debug
        assert dbe.eval("1") == 1
        assert type(dbe.eval("1")) is int
        x = dbe.eval("1.2")
        assert x == 1.2, x
