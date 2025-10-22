import pytest

from dl_formula_testing.evaluator import DbEvaluator
from dl_formula_testing.testcases.functions_native import DefaultNativeFunctionFormulaConnectorTestSuite

from dl_connector_ydb_tests.db.formula.base import YQLTestBase


class TestNativeFunctionYdb(
    YQLTestBase,
    DefaultNativeFunctionFormulaConnectorTestSuite,
):
    def test_native_functions(self, dbe: DbEvaluator) -> None:
        # DB_CALL_INT - using YQL module syntax
        assert dbe.eval('DB_CALL_INT("Math::Abs", -5)') == 5
        assert dbe.eval('DB_CALL_INT("String::Length", "hello")') == 5

        # DB_CALL_FLOAT - using YQL module syntax
        assert dbe.eval('DB_CALL_FLOAT("Math::Abs", -5.0)') == pytest.approx(5.0)
        assert dbe.eval('DB_CALL_FLOAT("Math::Floor", 5.7)') == pytest.approx(5.0)

        # DB_CALL_STRING - using YQL module syntax
        assert dbe.eval('DB_CALL_STRING("String::ToUpper", "hello")') == "HELLO"

        # DB_CALL_BOOL - using YQL module syntax
        assert dbe.eval('DB_CALL_BOOL("String::Contains", "hello world", "world")') == True
        assert dbe.eval('DB_CALL_BOOL("String::StartsWith", "hello", "he")') == True

        # DB_CALL_ARRAY_INT - using YQL list functions
        assert dbe.eval('DB_CALL_ARRAY_INT("AsList", 1, 2, 3, 4, 5)') == [1, 2, 3, 4, 5]

        # DB_CALL_ARRAY_FLOAT - using YQL list functions
        res = dbe.eval('DB_CALL_ARRAY_FLOAT("AsList", 1.0, 2.0, 3.0)')
        assert isinstance(res, list)
        assert len(res) == 3
        assert all(isinstance(x, float) for x in res)

        # DB_CALL_ARRAY_STRING - using YQL list functions
        assert dbe.eval('DB_CALL_ARRAY_STRING("AsList", "a", "b", "c")') == ["a", "b", "c"]
