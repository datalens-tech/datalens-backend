import pytest

from bi_formula.testing.evaluator import DbEvaluator
from bi_formula.connectors.base.testing.functions_logical import (
    DefaultLogicalFunctionFormulaConnectorTestSuite,
)
from bi_connector_clickhouse_tests.db.formula.base import ClickHouse_21_8TestBase


class LogicalFunctionClickHouseTestSuite(DefaultLogicalFunctionFormulaConnectorTestSuite):
    supports_nan_funcs = True
    supports_iif = True

    @pytest.mark.skip('Not fixed in ClickHouse yet')
    def test_bi_1052(self, dbe: DbEvaluator) -> None:
        assert dbe.eval('IF MONTH(DATE("1989-03-17")) = 8 THEN "first" ELSE "second" END')


class TestMainAggFunctionClickHouse_21_8(ClickHouse_21_8TestBase, LogicalFunctionClickHouseTestSuite):
    pass
