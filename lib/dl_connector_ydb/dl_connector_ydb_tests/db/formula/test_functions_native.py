import pytest

from dl_formula_testing.evaluator import DbEvaluator
from dl_formula_testing.testcases.functions_native import DefaultNativeFunctionFormulaConnectorTestSuite

from dl_connector_ydb_tests.db.formula.base import YQLTestBase


class TestNativeFunctionYdb(
    YQLTestBase,
    DefaultNativeFunctionFormulaConnectorTestSuite,
):
    def test_native_functions(self, dbe: DbEvaluator) -> None:
        # DB_CALL_INT
        assert dbe.eval('DB_CALL_INT("ABS", -5)') == 5
        assert dbe.eval('DB_CALL_INT("LENGTH", "hello")') == 5

        # DB_CALL_FLOAT
        assert dbe.eval('DB_CALL_FLOAT("ABS", -5.0)') == 5.0
        assert dbe.eval('DB_CALL_FLOAT("SQRT", 100.0)') == pytest.approx(10.0)

        # DB_CALL_STRING
        assert dbe.eval('DB_CALL_STRING("UPPER", "hello")') == "HELLO"

        # DB_CALL_BOOL
        assert dbe.eval('DB_CALL_BOOL("COALESCE", True, False)') == True

        # DB_CALL_ARRAY_INT
        res = dbe.eval('DB_CALL_ARRAY_INT("ListCreate", 1, 2, 3, 4, 5)')
        assert isinstance(res, list)
        assert len(res) >= 3

        # DB_CALL_ARRAY_FLOAT
        res = dbe.eval('DB_CALL_ARRAY_FLOAT("ListCreate", 1.0, 2.0, 3.0)')
        assert isinstance(res, list)
        assert len(res) == 3
        assert all(isinstance(x, float) for x in res)

        # DB_CALL_ARRAY_STRING
        res = dbe.eval('DB_CALL_ARRAY_STRING("ListCreate", "a", "b", "c")')
        assert isinstance(res, list)
        assert len(res) == 3
