import pytest

from dl_formula_testing.evaluator import DbEvaluator

from dl_connector_trino_tests.db.formula.base import TrinoFormulaTestBase


class TestNativeFunctionTrino(
    TrinoFormulaTestBase,
):
    def test_native_functions(self, dbe: DbEvaluator) -> None:
        # DB_CALL_INT
        assert dbe.eval('DB_CALL_INT("sign", -5)') == -1
        assert dbe.eval('DB_CALL_INT("sign", 5)') == 1
        assert dbe.eval('DB_CALL_INT("strpos", "abracadabra", "br", 2)') == 9

        # DB_CALL_FLOAT
        assert dbe.eval('DB_CALL_FLOAT("sign", -5.0)') == -1.0
        assert dbe.eval('DB_CALL_FLOAT("sign", 5.0)') == 1.0
        assert dbe.eval('DB_CALL_FLOAT("log10", 33.0)') == pytest.approx(1.518514)
        assert isinstance(dbe.eval('DB_CALL_FLOAT("random")'), float)

        # DB_CALL_STRING
        assert dbe.eval('DB_CALL_STRING("soundex", "datalens")') == "D345"

        # DB_CALL_BOOL
        assert dbe.eval('DB_CALL_BOOL("is_finite", 5)') == True
        assert dbe.eval('DB_CALL_BOOL("is_finite", DB_CALL_FLOAT("infinity"))') == False

        # DB_CALL_ARRAY_INT
        assert dbe.eval('DB_CALL_ARRAY_INT("repeat", 1, 5)') == [1, 1, 1, 1, 1]
        assert dbe.eval('DB_CALL_ARRAY_INT("sequence", 1, 10, 3)') == [1, 4, 7, 10]

        # DB_CALL_ARRAY_FLOAT
        assert dbe.eval('DB_CALL_ARRAY_FLOAT("repeat", 1.0, 5)') == [1.0, 1.0, 1.0, 1.0, 1.0]

        # DB_CALL_ARRAY_STRING
        assert dbe.eval('DB_CALL_ARRAY_STRING("repeat", "a", 5)') == ["a", "a", "a", "a", "a"]
