import pytest

from dl_formula_testing.evaluator import DbEvaluator
from dl_formula_testing.testcases.functions_native import (
    DefaultNativeAggregationFunctionFormulaConnectorTestSuite,
    DefaultNativeFunctionFormulaConnectorTestSuite,
)

from dl_connector_oracle_tests.db.formula.base import OracleTestBase


class TestNativeFunctionOracle(
    OracleTestBase,
    DefaultNativeFunctionFormulaConnectorTestSuite,
):
    def test_native_functions(self, dbe: DbEvaluator) -> None:
        # DB_CALL_INT
        assert dbe.eval('DB_CALL_INT("SIGN", -5)') == -1
        assert dbe.eval('DB_CALL_INT("SIGN", 5)') == 1
        assert dbe.eval('DB_CALL_INT("INSTR", "hello", "l")') == 3

        # DB_CALL_FLOAT
        assert dbe.eval('DB_CALL_FLOAT("SIGN", -5.0)') == -1.0
        assert dbe.eval('DB_CALL_FLOAT("SIGN", 5.0)') == 1.0
        assert dbe.eval('DB_CALL_FLOAT("LOG", 10, 100.0)') == pytest.approx(2.0)

        # DB_CALL_STRING
        assert dbe.eval('DB_CALL_STRING("REVERSE", "hello")') == "olleh"


class TestNativeAggregationFunctionOracle(
    OracleTestBase,
    DefaultNativeAggregationFunctionFormulaConnectorTestSuite,
):
    pass
