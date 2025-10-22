import pytest

from dl_formula_testing.evaluator import DbEvaluator
from dl_formula_testing.testcases.functions_native import DefaultNativeFunctionFormulaConnectorTestSuite

from dl_connector_mssql_tests.db.formula.base import MSSQLTestBase


class TestNativeFunctionMSSQL(
    MSSQLTestBase,
    DefaultNativeFunctionFormulaConnectorTestSuite,
):
    def test_native_functions(self, dbe: DbEvaluator) -> None:
        # DB_CALL_INT
        assert dbe.eval('DB_CALL_INT("SIGN", -5)') == -1
        assert dbe.eval('DB_CALL_INT("SIGN", 5)') == 1
        assert dbe.eval('DB_CALL_INT("CHARINDEX", "l", "hello")') == 3

        # DB_CALL_FLOAT
        assert dbe.eval('DB_CALL_FLOAT("SIGN", -5.0)') == -1.0
        assert dbe.eval('DB_CALL_FLOAT("SIGN", 5.0)') == 1.0
        assert dbe.eval('DB_CALL_FLOAT("LOG10", 100.0)') == pytest.approx(2.0)

        # DB_CALL_STRING
        assert dbe.eval('DB_CALL_STRING("REVERSE", "hello")') == "olleh"

        # DB_CALL_BOOL
        assert dbe.eval('DB_CALL_BOOL("ISNUMERIC", "123")') == True

        # DB_CALL_ARRAY_INT
        assert dbe.eval('DB_CALL_ARRAY_INT("STRING_SPLIT", "1,2,3,4,5", ",")') == [1, 2, 3, 4, 5]

        # DB_CALL_ARRAY_FLOAT
        res = dbe.eval('DB_CALL_ARRAY_FLOAT("STRING_SPLIT", "1.0,2.0,3.0", ",")')
        assert isinstance(res, list)
        assert len(res) == 3
        assert all(isinstance(x, float) for x in res)

        # DB_CALL_ARRAY_STRING
        assert dbe.eval('DB_CALL_ARRAY_STRING("STRING_SPLIT", "a,b,c", ",")') == ["a", "b", "c"]
