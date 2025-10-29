import pytest

from dl_formula_testing.evaluator import DbEvaluator
from dl_formula_testing.testcases.functions_native import (
    DefaultNativeAggregationFunctionFormulaConnectorTestSuite,
    DefaultNativeFunctionFormulaConnectorTestSuite,
)

from dl_connector_bigquery_tests.ext.formula.base import BigQueryTestBase


class TestNativeFunctionBigQuery(
    BigQueryTestBase,
    DefaultNativeFunctionFormulaConnectorTestSuite,
):
    def test_native_functions(self, dbe: DbEvaluator) -> None:
        # DB_CALL_INT
        assert dbe.eval('DB_CALL_INT("SIGN", -5)') == -1
        assert dbe.eval('DB_CALL_INT("SIGN", 5)') == 1
        assert dbe.eval('DB_CALL_INT("STRPOS", "hello world", "world")') == 7

        # DB_CALL_FLOAT
        assert dbe.eval('DB_CALL_FLOAT("SIGN", -5.0)') == -1.0
        assert dbe.eval('DB_CALL_FLOAT("SIGN", 5.0)') == 1.0
        assert dbe.eval('DB_CALL_FLOAT("LOG10", 100.0)') == pytest.approx(2.0)

        # DB_CALL_STRING
        assert dbe.eval('DB_CALL_STRING("REVERSE", "hello")') == "olleh"

        # DB_CALL_BOOL
        assert dbe.eval('DB_CALL_BOOL("IS_INF", 5.0)') == False
        assert dbe.eval('DB_CALL_BOOL("IS_NAN", 5.0)') == False

        # DB_CALL_ARRAY_INT
        assert dbe.eval('DB_CALL_ARRAY_INT("GENERATE_ARRAY", 1, 5)') == [1, 2, 3, 4, 5]

        # DB_CALL_ARRAY_FLOAT
        res = dbe.eval('DB_CALL_ARRAY_FLOAT("ARRAY", 1.0, 2.0, 3.0)')
        assert isinstance(res, list)
        assert len(res) == 3
        assert all(isinstance(x, float) for x in res)

        # DB_CALL_ARRAY_STRING
        assert dbe.eval('DB_CALL_ARRAY_STRING("SPLIT", "a,b,c", ",")') == ["a", "b", "c"]


class TestNativeAggregationFunctionBigQuery(
    BigQueryTestBase,
    DefaultNativeAggregationFunctionFormulaConnectorTestSuite,
):
    pass
