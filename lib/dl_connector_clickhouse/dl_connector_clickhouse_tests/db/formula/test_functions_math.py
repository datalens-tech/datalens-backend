from dl_connector_clickhouse.formula.testing.test_suites import MathFunctionClickHouseTestSuite
from dl_connector_clickhouse_tests.db.formula.base import ClickHouse_21_8TestBase
from dl_formula_testing.evaluator import DbEvaluator

class TestMathFunctionClickHouse_21_8(ClickHouse_21_8TestBase, MathFunctionClickHouseTestSuite):
    def test_rounding_to_0(self, dbe: DbEvaluator) -> None:
        assert str(dbe.eval("CEILING(-0.1)")) == "0.0"
        assert str(dbe.eval("ROUND(-0.1)")) == "0.0"
        assert str(dbe.eval("ROUND(-0.1, 0)")) == "0.0"
