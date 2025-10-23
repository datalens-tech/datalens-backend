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
        assert dbe.eval('DB_CALL_INT("Math::Abs", -5)') == 5
        assert dbe.eval('DB_CALL_INT("Unicode::GetLength", "hello")') == 5

        # DB_CALL_FLOAT
        assert dbe.eval('DB_CALL_FLOAT("Math::Abs", -5.0)') == pytest.approx(5.0)
        assert dbe.eval('DB_CALL_FLOAT("Math::Floor", 5.7)') == pytest.approx(5.0)

        # DB_CALL_STRING
        assert dbe.eval('DB_CALL_STRING("Unicode::ToUpper", "hello")') == "HELLO"

        # DB_CALL_BOOL
        assert dbe.eval('DB_CALL_BOOL("String::Contains", "hello world", "world")') == True
        assert dbe.eval('DB_CALL_BOOL("String::StartsWith", "hello", "he")') == True

        pytest.xfail("YDB array support will be added in BI-6515")

        # DB_CALL_ARRAY_INT
        assert dbe.eval('DB_CALL_ARRAY_INT("AsList", 1, 2, 3, 4, 5)') == dbe.eval("ARRAY(1, 2, 3, 4, 5)")

        # DB_CALL_ARRAY_FLOAT
        assert dbe.eval('DB_CALL_ARRAY_FLOAT("AsList", 1.0, 2.0, 3.0)') == dbe.eval("ARRAY(1.0, 2.0, 3.0)")

        # DB_CALL_ARRAY_STRING
        assert dbe.eval('DB_CALL_ARRAY_STRING("AsList", "a", "b", "c")') == dbe.eval("ARRAY('a', 'b', 'c')")
