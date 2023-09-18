import datetime

from dl_connector_clickhouse_tests.db.formula.base import (
    ClickHouse_21_8TestBase,
    ClickHouse_22_10TestBase,
)
from dl_formula_testing.evaluator import DbEvaluator
from dl_formula_testing.testcases.operators import DefaultOperatorFormulaConnectorTestSuite


class TestOperatorClickHouse_21_8(ClickHouse_21_8TestBase, DefaultOperatorFormulaConnectorTestSuite):
    pass


class TestOperatorClickHouse_22_10(ClickHouse_22_10TestBase, DefaultOperatorFormulaConnectorTestSuite):
    def test_date_before_1970_add(self, dbe: DbEvaluator):
        assert dbe.eval("#1931-01-01# + 1") == datetime.date(1931, 1, 2)

    def test_date_before_1970_subtract(self, dbe: DbEvaluator):
        assert dbe.eval("#1931-01-03# - 1") == datetime.date(1931, 1, 2)

    def test_date_before_1970_diff(self, dbe: DbEvaluator):
        assert dbe.eval("#1931-01-03# - #1931-01-01#") == 2

    def test_date_before_and_after_1970_diff(self, dbe: DbEvaluator):
        assert dbe.eval("#1971-01-03# - #1931-01-03#") == 14610
