from dl_connector_clickhouse_tests.db.formula.base import ClickHouse_21_8TestBase
from dl_formula_testing.evaluator import DbEvaluator
from dl_formula_testing.testcases.functions_math import DefaultMathFunctionFormulaConnectorTestSuite


class MathFunctionClickHouseTestSuite(DefaultMathFunctionFormulaConnectorTestSuite):
    def test_compare(self, dbe: DbEvaluator) -> None:
        assert dbe.eval("COMPARE(0, 0, 0)") == 0
        assert dbe.eval("COMPARE(0.123, 0.123, 0)") == 0
        assert dbe.eval("COMPARE(0.123, 0.123, 0.1)") == 0
        assert dbe.eval("COMPARE(0.123, 0.12, 0.1)") == 0
        assert dbe.eval("COMPARE(0.123, 0.12, 0.001)") == 1
        assert dbe.eval("COMPARE(0.12, 0.123, 0.001)") == -1


class TestMathFunctionClickHouse_21_8(ClickHouse_21_8TestBase, MathFunctionClickHouseTestSuite):
    pass
