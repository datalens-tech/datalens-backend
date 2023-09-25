from functools import reduce

import pytest
import sqlalchemy as sa

from dl_connector_clickhouse_tests.db.formula.base import ClickHouse_21_8TestBase
from dl_formula_testing.evaluator import DbEvaluator
from dl_formula_testing.testcases.functions_array import DefaultArrayFunctionFormulaConnectorTestSuite


class ArrayFunctionClickHouseTestSuite(DefaultArrayFunctionFormulaConnectorTestSuite):
    def test_startswith_string_array(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval("STARTSWITH([arr_str_value], [arr_str_value])", from_=data_table)
        assert not dbe.eval('STARTSWITH([arr_str_value], ARRAY("", "cde", NULL))', from_=data_table)


class TestArrayFunctionClickHouse_21_8(ClickHouse_21_8TestBase, ArrayFunctionClickHouseTestSuite):
    def test_array_contains_subsequence(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval("CONTAINS_SUBSEQUENCE(ARRAY(1, 2, 3), ARRAY(1, 2))", from_=data_table)
        assert dbe.eval("CONTAINS_SUBSEQUENCE(ARRAY(1.1, 2.2, 3.3), ARRAY(1.1, 2.2))", from_=data_table)
        assert dbe.eval('CONTAINS_SUBSEQUENCE(ARRAY("a", "b", "c"), ARRAY("a", "b"))', from_=data_table)

        assert not dbe.eval("CONTAINS_SUBSEQUENCE(ARRAY(1, 2, 3), ARRAY(1, 3))", from_=data_table)
        assert not dbe.eval("CONTAINS_SUBSEQUENCE(ARRAY(1.1, 2.2, 3.3), ARRAY(2.2, 1.1))", from_=data_table)
        assert not dbe.eval('CONTAINS_SUBSEQUENCE(ARRAY("a", "b", "c"), ARRAY("a", "a"))', from_=data_table)
        assert not dbe.eval('CONTAINS_SUBSEQUENCE(ARRAY("a", "b", "c"), ARRAY("a", "e"))', from_=data_table)

        assert dbe.eval('CONTAINS_SUBSEQUENCE(ARRAY("a", NULL, "c"), ARRAY("a", NULL))', from_=data_table)
        assert not dbe.eval('CONTAINS_SUBSEQUENCE(ARRAY("a", NULL, "c"), ARRAY("a", "c"))', from_=data_table)
        assert not dbe.eval("CONTAINS_SUBSEQUENCE(ARRAY(1.1, 2.2, 3.3), ARRAY(1.1, NULL))", from_=data_table)

        assert dbe.eval("CONTAINS_SUBSEQUENCE([arr_int_value], ARRAY(23, 456))", from_=data_table)
        assert not dbe.eval("CONTAINS_SUBSEQUENCE([arr_int_value], ARRAY(24, 456))", from_=data_table)

        assert dbe.eval("CONTAINS_SUBSEQUENCE([arr_int_value], SLICE([arr_int_value], 2, 1))", from_=data_table)
        assert dbe.eval("CONTAINS_SUBSEQUENCE(ARRAY(0, 23, 456, NULL, 123), [arr_int_value])", from_=data_table)

        assert dbe.eval('CONTAINS_SUBSEQUENCE([arr_str_value], ARRAY("cde"))', from_=data_table)
        assert dbe.eval('CONTAINS_SUBSEQUENCE([arr_str_value], ARRAY("cde", NULL))', from_=data_table)
        assert not dbe.eval('CONTAINS_SUBSEQUENCE(ARRAY("cde"), [arr_str_value])', from_=data_table)

    @pytest.mark.parametrize(
        "bi_func, eval_func",
        [
            ("ARR_MIN", min),
            ("ARR_MAX", max),
            ("ARR_SUM", sum),
            ("ARR_AVG", lambda a: sum(a) / len(a)),
            ("ARR_PRODUCT", lambda a: reduce(lambda x, y: x * y, a, 1)),
        ],
    )
    def test_array_aggregation_functions(self, dbe: DbEvaluator, data_table: sa.Table, bi_func, eval_func):
        """
        Array aggregation functions (arrayMin/Max/Sum/Avg) were added in CH 21.1
        (arrayProduct in 21.6).
        """

        inp_int = (1, 2, 3, -1)
        inp_float = (1.2, 12, 0.1, 12.0)

        bi_inp_int = ", ".join((str(item) for item in inp_int))
        bi_inp_float = ", ".join((str(item) for item in inp_float))

        assert dbe.eval(f"{bi_func}(ARRAY({bi_inp_int}))", from_=data_table) == eval_func(inp_int)
        assert dbe.eval(f"{bi_func}(REPLACE([arr_int_value], NULL, 1))", from_=data_table) == eval_func((0, 23, 456, 1))
        assert dbe.eval(f"{bi_func}(ARRAY({bi_inp_float}))", from_=data_table) == eval_func(inp_float)
        assert dbe.eval(f"{bi_func}(REPLACE([arr_float_value], NULL, 1))", from_=data_table) == eval_func(
            (0, 45, 0.123, 1)
        )
