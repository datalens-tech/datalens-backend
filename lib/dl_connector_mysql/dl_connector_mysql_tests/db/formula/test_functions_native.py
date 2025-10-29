import pytest

from dl_formula_testing.evaluator import DbEvaluator
from dl_formula_testing.testcases.functions_native import (
    DefaultNativeAggregationFunctionFormulaConnectorTestSuite,
    DefaultNativeFunctionFormulaConnectorTestSuite,
)

from dl_connector_mysql_tests.db.formula.base import (
    MySQL_5_7TestBase,
    MySQL_8_0_12TestBase,
)


class TestNativeFunctionMySQL_5_7(
    MySQL_5_7TestBase,
    DefaultNativeFunctionFormulaConnectorTestSuite,
):
    def test_native_functions(self, dbe: DbEvaluator) -> None:
        # DB_CALL_INT
        assert dbe.eval('DB_CALL_INT("SIGN", -5)') == -1
        assert dbe.eval('DB_CALL_INT("SIGN", 5)') == 1
        assert dbe.eval('DB_CALL_INT("LOCATE", "l", "hello")') == 3

        # DB_CALL_FLOAT
        assert dbe.eval('DB_CALL_FLOAT("SIGN", -5.0)') == -1.0
        assert dbe.eval('DB_CALL_FLOAT("SIGN", 5.0)') == 1.0
        assert dbe.eval('DB_CALL_FLOAT("LOG10", 100.0)') == pytest.approx(2.0)

        # DB_CALL_STRING
        assert dbe.eval('DB_CALL_STRING("REVERSE", "hello")') == "olleh"

        # DB_CALL_BOOL
        assert dbe.eval('DB_CALL_BOOL("ISNULL", 5)') == False


class TestNativeFunctionMySQL_8_0_12(
    MySQL_8_0_12TestBase,
    DefaultNativeFunctionFormulaConnectorTestSuite,
):
    def test_native_functions(self, dbe: DbEvaluator) -> None:
        # DB_CALL_INT
        assert dbe.eval('DB_CALL_INT("SIGN", -5)') == -1
        assert dbe.eval('DB_CALL_INT("SIGN", 5)') == 1
        assert dbe.eval('DB_CALL_INT("LOCATE", "l", "hello")') == 3

        # DB_CALL_FLOAT
        assert dbe.eval('DB_CALL_FLOAT("SIGN", -5.0)') == -1.0
        assert dbe.eval('DB_CALL_FLOAT("SIGN", 5.0)') == 1.0
        assert dbe.eval('DB_CALL_FLOAT("LOG10", 100.0)') == pytest.approx(2.0)

        # DB_CALL_STRING
        assert dbe.eval('DB_CALL_STRING("REVERSE", "hello")') == "olleh"

        # DB_CALL_BOOL
        assert dbe.eval('DB_CALL_BOOL("ISNULL", 5)') == False


class TestNativeAggregationFunctionMySQL_5_7(
    MySQL_5_7TestBase,
    DefaultNativeAggregationFunctionFormulaConnectorTestSuite,
):
    pass


class TestNativeAggregationFunctionMySQL_8_0_12(
    MySQL_8_0_12TestBase,
    DefaultNativeAggregationFunctionFormulaConnectorTestSuite,
):
    pass
