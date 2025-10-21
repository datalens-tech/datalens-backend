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
        # assert dbe.eval('DB_CALL_INT("sign", -5)') == -1

        # DB_CALL_FLOAT
        # assert dbe.eval('DB_CALL_FLOAT("sign", -5.0)') == -1.0

        # DB_CALL_STRING
        # assert dbe.eval('DB_CALL_STRING("soundex", "datalens")') == "D345"

        # DB_CALL_BOOL
        # assert dbe.eval('DB_CALL_BOOL("is_finite", 5)') == True

        # DB_CALL_ARRAY_INT
        # assert dbe.eval('DB_CALL_ARRAY_INT("repeat", 1, 5)') == [1, 1, 1, 1, 1]

        # DB_CALL_ARRAY_FLOAT
        # assert dbe.eval('DB_CALL_ARRAY_FLOAT("repeat", 1.0, 5)') == [1.0, 1.0, 1.0, 1.0, 1.0]

        # DB_CALL_ARRAY_STRING
        # assert dbe.eval('DB_CALL_ARRAY_STRING("repeat", "a", 5)') == ["a", "a", "a", "a", "a"]
        raise NotImplementedError


class TestNativeFunctionPostgreSQL_9_4(
    PostgreSQL_9_4TestBase,
    DefaultNativeFunctionFormulaConnectorTestSuite,
):
    def test_native_functions(self, dbe: DbEvaluator) -> None:
        # DB_CALL_INT
        # assert dbe.eval('DB_CALL_INT("sign", -5)') == -1

        # DB_CALL_FLOAT
        # assert dbe.eval('DB_CALL_FLOAT("sign", -5.0)') == -1.0

        # DB_CALL_STRING
        # assert dbe.eval('DB_CALL_STRING("soundex", "datalens")') == "D345"

        # DB_CALL_BOOL
        # assert dbe.eval('DB_CALL_BOOL("is_finite", 5)') == True

        # DB_CALL_ARRAY_INT
        # assert dbe.eval('DB_CALL_ARRAY_INT("repeat", 1, 5)') == [1, 1, 1, 1, 1]

        # DB_CALL_ARRAY_FLOAT
        # assert dbe.eval('DB_CALL_ARRAY_FLOAT("repeat", 1.0, 5)') == [1.0, 1.0, 1.0, 1.0, 1.0]

        # DB_CALL_ARRAY_STRING
        # assert dbe.eval('DB_CALL_ARRAY_STRING("repeat", "a", 5)') == ["a", "a", "a", "a", "a"]
        raise NotImplementedError
