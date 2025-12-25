import pytest
import sqlalchemy as sa

from dl_formula_testing.evaluator import DbEvaluator
from dl_formula_testing.testcases.functions_native import (
    DefaultNativeAggregationFunctionFormulaConnectorTestSuite,
    DefaultNativeFunctionFormulaConnectorTestSuite,
)

from dl_connector_clickhouse_tests.db.formula.base import (
    ClickHouse_21_8TestBase,
    ClickHouse_22_10TestBase,
)


class TestNativeFunctionClickHouse_21_8(
    ClickHouse_21_8TestBase,
    DefaultNativeFunctionFormulaConnectorTestSuite,
):
    def test_native_functions(self, dbe: DbEvaluator) -> None:
        # DB_CALL_INT
        assert dbe.eval('DB_CALL_INT("sign", -5)') == -1
        assert dbe.eval('DB_CALL_INT("sign", 5)') == 1
        assert dbe.eval('DB_CALL_INT("positionCaseInsensitive", "Hello", "l")') == 3
        assert dbe.eval('DB_CALL_INT("CAST", "3", "UInt32")') == 3

        # DB_CALL_FLOAT
        assert dbe.eval('DB_CALL_FLOAT("sign", -5.0)') == -1.0
        assert dbe.eval('DB_CALL_FLOAT("sign", 5.0)') == 1.0
        assert dbe.eval('DB_CALL_FLOAT("log10", 100.0)') == pytest.approx(2.0)

        # DB_CALL_STRING
        assert dbe.eval('DB_CALL_STRING("reverse", "hello")') == "olleh"
        assert dbe.eval('DB_CALL_STRING("CAST", 3, "String")') == "3"

        # DB_CALL_BOOL
        assert dbe.eval('DB_CALL_BOOL("isFinite", 5)') == True
        assert dbe.eval('DB_CALL_BOOL("isInfinite", 5)') == False

        # DB_CALL_ARRAY_INT
        assert dbe.eval('DB_CALL_ARRAY_INT("range", 5)') == dbe.eval("ARRAY(0, 1, 2, 3, 4)")

        # DB_CALL_ARRAY_FLOAT
        assert dbe.eval('DB_CALL_ARRAY_FLOAT("arrayConcat", ARRAY(1.0, 2.0), ARRAY(3.0))') == dbe.eval(
            "ARRAY(1.0, 2.0, 3.0)"
        )

        # DB_CALL_ARRAY_STRING
        assert dbe.eval('DB_CALL_ARRAY_STRING("splitByChar", ",", "a,b,c")') == dbe.eval('ARRAY("a", "b", "c")')


class TestNativeFunctionClickHouse_22_10(
    ClickHouse_22_10TestBase,
    DefaultNativeFunctionFormulaConnectorTestSuite,
):
    def test_native_functions(self, dbe: DbEvaluator) -> None:
        # DB_CALL_INT
        assert dbe.eval('DB_CALL_INT("sign", -5)') == -1
        assert dbe.eval('DB_CALL_INT("sign", 5)') == 1
        assert dbe.eval('DB_CALL_INT("positionCaseInsensitive", "Hello", "l")') == 3

        # DB_CALL_FLOAT
        assert dbe.eval('DB_CALL_FLOAT("sign", -5.0)') == -1.0
        assert dbe.eval('DB_CALL_FLOAT("sign", 5.0)') == 1.0
        assert dbe.eval('DB_CALL_FLOAT("log10", 100.0)') == pytest.approx(2.0)

        # DB_CALL_STRING
        assert dbe.eval('DB_CALL_STRING("reverse", "hello")') == "olleh"

        # DB_CALL_BOOL
        assert dbe.eval('DB_CALL_BOOL("isFinite", 5)') == True
        assert dbe.eval('DB_CALL_BOOL("isInfinite", 5)') == False

        # DB_CALL_ARRAY_INT
        assert dbe.eval('DB_CALL_ARRAY_INT("range", 5)') == dbe.eval("ARRAY(0, 1, 2, 3, 4)")

        # DB_CALL_ARRAY_FLOAT
        assert dbe.eval('DB_CALL_ARRAY_FLOAT("arrayConcat", ARRAY(1.0, 2.0), ARRAY(3.0))') == dbe.eval(
            "ARRAY(1.0, 2.0, 3.0)"
        )

        # DB_CALL_ARRAY_STRING
        assert dbe.eval('DB_CALL_ARRAY_STRING("splitByChar", ",", "a,b,c")') == dbe.eval('ARRAY("a", "b", "c")')


class TestNativeAggregationFunctionClickHouse_21_8(
    ClickHouse_21_8TestBase,
    DefaultNativeAggregationFunctionFormulaConnectorTestSuite,
):
    pass


class TestNativeAggregationFunctionClickHouse_22_10(
    ClickHouse_22_10TestBase,
    DefaultNativeAggregationFunctionFormulaConnectorTestSuite,
):
    def test_native_aggregation_functions(
        self,
        dbe: DbEvaluator,
        data_table: sa.Table,
        native_agg_function_names: dict[str, str],
    ) -> None:
        int_values = data_table.int_values  # type: ignore  # 2025-10-27 # TODO: "Table" has no attribute "int_values"  [attr-defined]
        str_values = data_table.str_values  # type: ignore  # 2025-10-27 # TODO: "Table" has no attribute "str_values"  [attr-defined]

        assert (
            dbe.eval('DB_CALL_AGG_INT("argMax", [str_value], [int_value])', from_=data_table)
            == max(list(zip(int_values, str_values)))[1]
        )
        assert dbe.eval('DB_CALL_AGG_INT("sumIf", [int_value], [str_value] = "qqqqq")', from_=data_table) == sum(
            next(zip(*filter(lambda x: x[1] == "qqqqq", zip(int_values, str_values))))
        )
