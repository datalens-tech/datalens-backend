import sqlalchemy as sa

from bi_formula.testing.evaluator import DbEvaluator
from bi_formula.connectors.base.testing.functions_aggregation import (
    DefaultMainAggFunctionFormulaConnectorTestSuite,
)
from bi_connector_clickhouse_tests.db.formula.base import ClickHouse_21_8TestBase


class MainAggFunctionClickHouseTestSuite(DefaultMainAggFunctionFormulaConnectorTestSuite):
    supports_countd_approx = True
    supports_quantile = True
    supports_median = True
    supports_arg_min_max = True
    supports_any = True
    supports_all_concat = True
    supports_top_concat = True

    def test_quantile(self, dbe: DbEvaluator, data_table: sa.Table) -> None:  # additional checks for CH
        value = dbe.eval('QUANTILE([int_value], 0.9)', from_=data_table)
        assert 80 <= value <= 90
        assert abs(value - 90) < 0.5  # allow for float approximation.

        value = dbe.eval('QUANTILE_APPROX([int_value], 0.9)', from_=data_table)
        assert 80 < value < 91  # can be either 81 or 90, apparently.


class TestMainAggFunctionClickHouse_21_8(ClickHouse_21_8TestBase, MainAggFunctionClickHouseTestSuite):
    pass
