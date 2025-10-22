import pytest

from dl_formula_testing.evaluator import DbEvaluator
from dl_formula_testing.testcases.functions_native import DefaultNativeFunctionFormulaConnectorTestSuite

from dl_connector_postgresql_tests.db.formula.base import (
    PostgreSQL_9_3TestBase,
    PostgreSQL_9_4TestBase,
)


class TestNativeFunctionPostgreSQL_9_3(
    PostgreSQL_9_3TestBase,
    DefaultNativeFunctionFormulaConnectorTestSuite,
):
    def test_native_functions(self, dbe: DbEvaluator) -> None:
        # DB_CALL_INT
        assert dbe.eval('DB_CALL_INT("sign", -5)') == -1
        assert dbe.eval('DB_CALL_INT("sign", 5)') == 1
        assert dbe.eval('DB_CALL_INT("strpos", "hello", "l")') == 3

        # DB_CALL_FLOAT
        assert dbe.eval('DB_CALL_FLOAT("sign", -5.0)') == -1.0
        assert dbe.eval('DB_CALL_FLOAT("sign", 5.0)') == 1.0
        assert dbe.eval('DB_CALL_FLOAT("log", 100.0)') == pytest.approx(2.0)

        # DB_CALL_STRING
        assert dbe.eval('DB_CALL_STRING("reverse", "hello")') == "olleh"

        # DB_CALL_BOOL
        assert dbe.eval('DB_CALL_BOOL("isfinite", 5.0)') == True

        # DB_CALL_ARRAY_INT
        assert dbe.eval('DB_CALL_ARRAY_INT("array_append", ARRAY(1, 2, 3), 4)') == [1, 2, 3, 4]

        # DB_CALL_ARRAY_FLOAT
        res = dbe.eval('DB_CALL_ARRAY_FLOAT("array_append", ARRAY(1.0, 2.0), 3.0)')
        assert isinstance(res, list)
        assert len(res) == 3
        assert all(isinstance(x, float) for x in res)

        # DB_CALL_ARRAY_STRING
        assert dbe.eval('DB_CALL_ARRAY_STRING("string_to_array", "a,b,c", ",")') == ["a", "b", "c"]


class TestNativeFunctionPostgreSQL_9_4(
    PostgreSQL_9_4TestBase,
    DefaultNativeFunctionFormulaConnectorTestSuite,
):
    def test_native_functions(self, dbe: DbEvaluator) -> None:
        # DB_CALL_INT
        assert dbe.eval('DB_CALL_INT("sign", -5)') == -1
        assert dbe.eval('DB_CALL_INT("sign", 5)') == 1
        assert dbe.eval('DB_CALL_INT("strpos", "hello", "l")') == 3

        # DB_CALL_FLOAT
        assert dbe.eval('DB_CALL_FLOAT("sign", -5.0)') == -1.0
        assert dbe.eval('DB_CALL_FLOAT("sign", 5.0)') == 1.0
        assert dbe.eval('DB_CALL_FLOAT("log", 100.0)') == pytest.approx(2.0)

        # DB_CALL_STRING
        assert dbe.eval('DB_CALL_STRING("reverse", "hello")') == "olleh"

        # DB_CALL_BOOL
        assert dbe.eval('DB_CALL_BOOL("isfinite", 5.0)') == True

        # DB_CALL_ARRAY_INT
        assert dbe.eval('DB_CALL_ARRAY_INT("array_append", ARRAY(1, 2, 3), 4)') == [1, 2, 3, 4]

        # DB_CALL_ARRAY_FLOAT
        res = dbe.eval('DB_CALL_ARRAY_FLOAT("array_append", ARRAY(1.0, 2.0), 3.0)')
        assert isinstance(res, list)
        assert len(res) == 3
        assert all(isinstance(x, float) for x in res)

        # DB_CALL_ARRAY_STRING
        assert dbe.eval('DB_CALL_ARRAY_STRING("string_to_array", "a,b,c", ",")') == ["a", "b", "c"]
