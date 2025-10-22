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
        assert dbe.eval('DB_CALL_INT("Math::Sign", -5)') == -1
        assert dbe.eval('DB_CALL_INT("Math::Sign", 5)') == 1
        assert dbe.eval('DB_CALL_INT("String::Find", "hello", "l")') == 2

        # DB_CALL_FLOAT
        assert dbe.eval('DB_CALL_FLOAT("Math::Sign", -5.0)') == -1.0
        assert dbe.eval('DB_CALL_FLOAT("Math::Sign", 5.0)') == 1.0
        assert dbe.eval('DB_CALL_FLOAT("Math::Log10", 100.0)') == pytest.approx(2.0)

        # DB_CALL_STRING
        assert dbe.eval('DB_CALL_STRING("String::Reverse", "hello")') == "olleh"

        # DB_CALL_BOOL
        assert dbe.eval('DB_CALL_BOOL("String::Contains", "hello world", "world")') == True

        # DB_CALL_ARRAY_INT
        assert dbe.eval('DB_CALL_ARRAY_INT("ListFromRange", 0, 5)') == [0, 1, 2, 3, 4]

        # DB_CALL_ARRAY_FLOAT
        res = dbe.eval('DB_CALL_ARRAY_FLOAT("AsList", 1.0, 2.0, 3.0)')
        assert isinstance(res, list)
        assert len(res) == 3
        assert all(isinstance(x, float) for x in res)

        # DB_CALL_ARRAY_STRING
        assert dbe.eval('DB_CALL_ARRAY_STRING("String::SplitToList", "a,b,c", ",")') == ["a", "b", "c"]
