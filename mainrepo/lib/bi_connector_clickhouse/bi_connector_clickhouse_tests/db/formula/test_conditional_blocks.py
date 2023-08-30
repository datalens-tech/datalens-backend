from bi_formula.testing.evaluator import DbEvaluator
from bi_formula.connectors.base.testing.conditional_blocks import (
    DefaultConditionalBlockFormulaConnectorTestSuite,
)
from bi_formula.testing.util import to_str

from bi_connector_clickhouse_tests.db.formula.base import ClickHouse_21_8TestBase


class ConditionalBlockClickHouseTestSuite(DefaultConditionalBlockFormulaConnectorTestSuite):
    def test_case_block_returning_null(self, dbe: DbEvaluator):
        # Workaround for https://st.yandex-team.ru/CLICKHOUSE-4376
        assert to_str(dbe.eval('CASE 3 WHEN 1 THEN "1st" WHEN 2 THEN "2nd" END')) == ''
        assert dbe.eval('CASE 3 WHEN 1 THEN 1 WHEN 2 THEN 2 END') == 0
        assert dbe.eval('CASE 3 WHEN 1 THEN 1.1 WHEN 2 THEN 2.2 END') == 0.0
        assert dbe.eval('CASE 3 WHEN 1 THEN TRUE WHEN 2 THEN TRUE END') is False
        # NULL in THEN
        assert to_str(dbe.eval('CASE 2 WHEN 1 THEN NULL WHEN 2 THEN "2nd" ELSE "3rd" END')) == '2nd'
        assert to_str(dbe.eval('CASE 1 WHEN 1 THEN NULL WHEN 2 THEN "2nd" ELSE "3rd" END')) == ''


class TestMainAggFunctionClickHouse_21_8(ClickHouse_21_8TestBase, ConditionalBlockClickHouseTestSuite):
    pass
