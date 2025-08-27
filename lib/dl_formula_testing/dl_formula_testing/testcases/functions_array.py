import abc
from typing import (
    Any,
    ClassVar,
    Optional,
    Union,
)

import pytest
import sqlalchemy as sa

import dl_formula.core.exc as exc
from dl_formula_testing.evaluator import DbEvaluator
from dl_formula_testing.testcases.base import FormulaConnectorTestBase
from dl_formula_testing.util import to_str


SAMPLE_DATA_ARRAYS_LENGTH = 4


class DefaultArrayFunctionFormulaConnectorTestSuite(FormulaConnectorTestBase):
    make_decimal_cast: ClassVar[Optional[str]] = None
    make_float_cast: ClassVar[Optional[str]] = None
    make_float_array_cast: ClassVar[Optional[str]] = None
    make_str_array_cast: ClassVar[Optional[str]] = None

    def test_create_array(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        sample_array: tuple[Union[str, int, float, None], ...]
        expected: tuple[Union[str, int, float, None], ...]
        item: Any
        exp: Any
        for sample_array in (
            (0, 23, 456, None),
            (0, 43, 0.123, None),
        ):
            array_string = ",".join("NULL" if item is None else str(item) for item in sample_array)
            for i, item in enumerate(sample_array):
                assert dbe.eval(f"GET_ITEM(ARRAY({array_string}), {i + 1})") == item
                assert dbe.eval(f"GET_ITEM(ARRAY({array_string}), {i + 1})") == item

        # testing string separately because they must be quoted
        sample_array = ("", "NULL", "cde", None)
        array_string = ",".join("NULL" if item is None else f"'{item}'" for item in sample_array)
        for i, item in enumerate(sample_array):
            assert dbe.eval(f"GET_ITEM(ARRAY({array_string}), {i + 1})") == item
            assert dbe.eval(f"GET_ITEM(ARRAY({array_string}), {i + 1})") == item

        # it is not allowed to create array with undefined element type
        with pytest.raises(exc.TranslationError):
            dbe.eval("ARRAY(NULL, NULL, NULL)")

        expected = (0, 1, None)
        for i, exp in enumerate(expected):
            assert dbe.eval(f"GET_ITEM(ARRAY([int_value], 1, NULL), {i + 1})", from_=data_table) == exp

        expected = ("q", "1", None)
        for i, exp in enumerate(expected):
            assert dbe.eval(f"GET_ITEM(ARRAY([str_value], STR(1), NULL), {i + 1})", from_=data_table) == exp

    def test_unnest_array(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        for array_field, inner_type in (
            ("arr_int_value", int),
            ("arr_float_value", float),
            ("arr_str_value", str),
        ):
            count_before_unnest = len(dbe.eval(f"[{array_field}]", from_=data_table, many=True))
            res = dbe.eval(f"UNNEST([{array_field}])", from_=data_table, many=True)
            assert len(res) == count_before_unnest * SAMPLE_DATA_ARRAYS_LENGTH
            assert all(isinstance(value, inner_type) or value is None for value in res)

    def test_array_len(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval("LEN([arr_int_value])", from_=data_table) == SAMPLE_DATA_ARRAYS_LENGTH
        assert dbe.eval("LEN([arr_float_value])", from_=data_table) == SAMPLE_DATA_ARRAYS_LENGTH
        assert dbe.eval("LEN([arr_str_value])", from_=data_table) == SAMPLE_DATA_ARRAYS_LENGTH

    @pytest.mark.xfail(reason="BI-6163, BI-6165")
    def test_array_len_null(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        # null array values
        assert dbe.eval("LEN([arr_int_null_value])", from_=data_table) is None
        assert dbe.eval("LEN([arr_float_null_value])", from_=data_table) is None
        assert dbe.eval("LEN([arr_str_null_value])", from_=data_table) is None

    def test_array_get_item(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval("GET_ITEM([arr_int_value], 3)", from_=data_table) == 456
        assert dbe.eval("GET_ITEM([arr_float_value], 3)", from_=data_table) == 0.123
        assert dbe.eval("GET_ITEM([arr_str_value], 3)", from_=data_table) == "cde"

    @pytest.mark.xfail(reason="BI-6163, BI-6165")
    def test_array_get_item_out_of_bounds(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval("GET_ITEM(ARRAY(42), 99999)") is None
        assert dbe.eval("GET_ITEM(ARRAY(1.23), 99999)") is None
        assert dbe.eval('GET_ITEM(ARRAY("cat"), 99999)') is None

    @pytest.mark.xfail(reason="BI-6163, BI-6165")
    def test_array_get_item_null(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        # null array values
        assert dbe.eval("GET_ITEM([arr_int_null_value], 0)", from_=data_table) is None
        assert dbe.eval("GET_ITEM([arr_float_null_value], 0)", from_=data_table) is None
        assert dbe.eval("GET_ITEM([arr_str_null_value], 0)", from_=data_table) is None

    def test_array_str(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert to_str(dbe.eval("ARR_STR([arr_int_value])", from_=data_table)) == "0,23,456"
        assert to_str(dbe.eval("ARR_STR([arr_float_value])", from_=data_table)) == "0,45,0.123"
        assert to_str(dbe.eval("ARR_STR([arr_str_value])", from_=data_table)) == ",,cde"

        assert to_str(dbe.eval("ARR_STR([arr_int_value], ';')", from_=data_table)) == "0;23;456"
        assert to_str(dbe.eval("ARR_STR([arr_float_value], ';')", from_=data_table)) == "0;45;0.123"
        assert to_str(dbe.eval("ARR_STR([arr_str_value], ';')", from_=data_table)) == ";;cde"

        assert to_str(dbe.eval("ARR_STR([arr_int_value], ';', '*')", from_=data_table)) == "0;23;456;*"
        assert to_str(dbe.eval("ARR_STR([arr_float_value], ';', '*')", from_=data_table)) == "0;45;0.123;*"
        assert to_str(dbe.eval("ARR_STR([arr_str_value], ';', '*')", from_=data_table)) == ";;cde;*"

    @pytest.mark.xfail(reason="BI-6163, BI-6165")
    def test_array_str_null(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        # null array values
        assert to_str(dbe.eval("ARR_STR([arr_int_null_value])", from_=data_table)) is None
        assert to_str(dbe.eval("ARR_STR([arr_float_null_value])", from_=data_table)) is None
        assert to_str(dbe.eval("ARR_STR([arr_str_null_value])", from_=data_table)) is None

        assert to_str(dbe.eval("ARR_STR([arr_int_null_value], ';')", from_=data_table)) is None
        assert to_str(dbe.eval("ARR_STR([arr_float_null_value], ';')", from_=data_table)) is None
        assert to_str(dbe.eval("ARR_STR([arr_str_null_value], ';')", from_=data_table)) is None

        assert to_str(dbe.eval("ARR_STR([arr_int_null_value], ';', '*')", from_=data_table)) is None
        assert to_str(dbe.eval("ARR_STR([arr_float_null_value], ';', '*')", from_=data_table)) is None
        assert to_str(dbe.eval("ARR_STR([arr_str_null_value], ';', '*')", from_=data_table)) is None

    def _float_array_cast(self, expression: str) -> str:
        if self.make_float_array_cast:
            return f'DB_CAST({expression}, "{self.make_float_array_cast}")'
        return expression

    def _str_array_cast(self, expression: str) -> str:
        if self.make_str_array_cast:
            return f'DB_CAST({expression}, "{self.make_str_array_cast}")'
        return expression

    def test_array_count_item_int(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval("COUNT_ITEM([arr_int_value], 23)", from_=data_table) == 1
        assert dbe.eval("COUNT_ITEM([arr_int_value], NULL)", from_=data_table) == 1
        # array expression
        assert dbe.eval("COUNT_ITEM(ARRAY(6, 7, 8, NULL), 7)") == 1
        assert dbe.eval("COUNT_ITEM(ARRAY(6, 7, 8, NULL), 9)") == 0
        assert dbe.eval("COUNT_ITEM(ARRAY(4, 9, 4), 4)") == 2
        assert dbe.eval("COUNT_ITEM(ARRAY(4, 4), 4)") == 2
        assert dbe.eval("COUNT_ITEM(ARRAY(4), 4)") == 1
        # NULL
        assert dbe.eval("COUNT_ITEM(ARRAY(3), NULL)") == 0
        assert dbe.eval("COUNT_ITEM(ARRAY(3, NULL), NULL)") == 1
        assert dbe.eval("COUNT_ITEM(ARRAY(3, NULL, NULL), NULL)") == 2

    @pytest.mark.xfail(reason="BI-6163, BI-6165")
    def test_array_count_item_int_null(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        # null array values
        assert dbe.eval("COUNT_ITEM([arr_int_null_value], 1)", from_=data_table) is None

    def test_array_count_item_float(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval("COUNT_ITEM([arr_float_value], 45)", from_=data_table) == 1
        assert dbe.eval("COUNT_ITEM([arr_float_value], 0.123)", from_=data_table) == 1
        # array expression
        assert dbe.eval("COUNT_ITEM(ARRAY(0.321, 19, NULL), 0.321)") == 1
        assert dbe.eval("COUNT_ITEM(ARRAY(0.321, 19, NULL), 42)") == 0
        assert dbe.eval("COUNT_ITEM(ARRAY(18.3, 17.6, 18.3), 18.3)") == 2
        assert dbe.eval("COUNT_ITEM(ARRAY(18.3, 18.3), 18.3)") == 2
        assert dbe.eval("COUNT_ITEM(ARRAY(18.3), 18.3)") == 1
        # NULL
        assert dbe.eval("COUNT_ITEM(ARRAY(0.2), NULL)") == 0
        assert dbe.eval("COUNT_ITEM(ARRAY(0.2, NULL), NULL)") == 1
        assert dbe.eval("COUNT_ITEM(ARRAY(0.2, NULL, NULL), NULL)") == 2

    @pytest.mark.xfail(reason="BI-6163, BI-6165")
    def test_array_count_item_float_null(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        # null array values
        assert dbe.eval("COUNT_ITEM([arr_float_null_value], 1.2)", from_=data_table) is None

    def test_array_count_item_str(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval('COUNT_ITEM([arr_str_value], "cde")', from_=data_table) == 1
        assert dbe.eval("COUNT_ITEM([arr_str_value], NULL)", from_=data_table) == 1
        # array expression
        assert dbe.eval('COUNT_ITEM(ARRAY("a", "bc", "de", NULL), NULL)') == 1
        assert dbe.eval('COUNT_ITEM(ARRAY("a", "bc", "de", NULL), "bc")') == 1
        assert dbe.eval('COUNT_ITEM(ARRAY("a", "bc", "de", NULL), "ad")') == 0
        assert dbe.eval('COUNT_ITEM(ARRAY("a", "a"), "a")') == 2
        assert dbe.eval('COUNT_ITEM(ARRAY("a", "b", "a"), "a")') == 2
        assert dbe.eval('COUNT_ITEM(ARRAY("a"), "a")') == 1
        # NULL
        assert dbe.eval('COUNT_ITEM(ARRAY("p"), NULL)') == 0
        assert dbe.eval('COUNT_ITEM(ARRAY("p", NULL), NULL)') == 1
        assert dbe.eval('COUNT_ITEM(ARRAY("p", NULL, NULL), NULL)') == 2
        # case-sensitive
        assert dbe.eval('COUNT_ITEM(ARRAY("a", "A"), "a")') == 1
        assert dbe.eval('COUNT_ITEM(ARRAY("A"), "a")') == 0
        assert dbe.eval('COUNT_ITEM(ARRAY("b"), "B")') == 0
        # "null"
        assert dbe.eval('COUNT_ITEM(ARRAY("NULL"), NULL)') == 0

    @pytest.mark.xfail(reason="BI-6163, BI-6165")
    def test_array_count_item_str_null(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        # null array values
        assert dbe.eval('COUNT_ITEM([arr_str_null_value], "cat")', from_=data_table) is None

    def test_array_contains_int(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval("CONTAINS(ARRAY(1, 2, 3, NULL), 1)")
        assert dbe.eval("CONTAINS(ARRAY(1, 2, 3), 1)")
        assert dbe.eval("CONTAINS(ARRAY(1, 2, 1), 1)")
        assert dbe.eval("CONTAINS(ARRAY(1, 1), 1)")
        assert dbe.eval("CONTAINS(ARRAY(1), 1)")
        # null
        assert not dbe.eval("CONTAINS(ARRAY(1), NULL)")
        assert dbe.eval("CONTAINS(ARRAY(1, NULL), NULL)")
        assert dbe.eval("CONTAINS(ARRAY(1, NULL, NULL), NULL)")
        assert dbe.eval("CONTAINS(ARRAY(1, 1, NULL, NULL), NULL)")

    @pytest.mark.xfail(reason="BI-6163, BI-6165")
    def test_array_contains_int_null(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        # null array values
        assert dbe.eval("CONTAINS([arr_int_null_value], 1)", from_=data_table) is None

    def test_array_contains_float(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval("CONTAINS(ARRAY(1, 2.4, 3, NULL), 2.4)")
        assert dbe.eval("CONTAINS(ARRAY(1, 2.4, 3), 2.4)")
        assert dbe.eval("CONTAINS(ARRAY(1, 2.4, 2.4), 2.4)")
        assert dbe.eval("CONTAINS(ARRAY(1.9, 1.9), 1.9)")
        assert dbe.eval("CONTAINS(ARRAY(1.9), 1.9)")
        # null
        assert not dbe.eval("CONTAINS(ARRAY(1.1), NULL)")
        assert dbe.eval("CONTAINS(ARRAY(1.1, NULL), NULL)")
        assert dbe.eval("CONTAINS(ARRAY(1.1, NULL, NULL), NULL)")
        assert dbe.eval("CONTAINS(ARRAY(1.1, 1.1, NULL, NULL), NULL)")

    @pytest.mark.xfail(reason="BI-6163, BI-6165")
    def test_array_contains_float_null(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        # null array values
        assert dbe.eval("CONTAINS([arr_float_null_value], 1.2)", from_=data_table) is None

    def test_array_contains_str(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval('CONTAINS(ARRAY("a", "bc", "ef", NULL), "a")')
        assert dbe.eval('CONTAINS(ARRAY("a", "bc", "ef"), "a")')
        assert dbe.eval('CONTAINS(ARRAY("a", "bc", "ef", "a", NULL), "a")')
        assert dbe.eval('CONTAINS(ARRAY("a", "bc", "ef", "a"), "a")')
        assert dbe.eval('CONTAINS(ARRAY("a"), "a")')
        assert dbe.eval('CONTAINS(ARRAY("a", "a"), "a")')
        # case-sensitive
        assert not dbe.eval('CONTAINS(ARRAY("a", "a", "B"), "b")')
        assert not dbe.eval('CONTAINS(ARRAY("a", "a", "b"), "B")')
        # null
        assert not dbe.eval('CONTAINS(ARRAY("a"), NULL)')
        assert dbe.eval('CONTAINS(ARRAY("a", NULL), NULL)')
        assert dbe.eval('CONTAINS(ARRAY("a", NULL, NULL), NULL)')
        assert dbe.eval('CONTAINS(ARRAY("a", "a", NULL, NULL), NULL)')

    @pytest.mark.xfail(reason="BI-6163, BI-6165")
    def test_array_contains_str_null(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        # null array values
        assert dbe.eval('CONTAINS([arr_str_null_value], "cat")', from_=data_table) is None

    def test_array_contains_column(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval("CONTAINS([arr_int_value], 23)", from_=data_table)
        assert not dbe.eval("CONTAINS([arr_int_value], 24)", from_=data_table)
        assert dbe.eval('CONTAINS([arr_str_value], "cde")', from_=data_table)
        assert dbe.eval("CONTAINS([arr_str_value], NULL)", from_=data_table)

        assert dbe.eval("CONTAINS([arr_str_value], GET_ITEM([arr_str_value], 1))", from_=data_table)
        assert dbe.eval("CONTAINS([arr_str_value], GET_ITEM([arr_str_value], 2))", from_=data_table)
        assert dbe.eval("CONTAINS([arr_str_value], GET_ITEM([arr_str_value], 3))", from_=data_table)
        assert dbe.eval("CONTAINS([arr_str_value], GET_ITEM([arr_str_value], 4))", from_=data_table)

    def test_array_contains_all_int(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval("CONTAINS_ALL(ARRAY(1, 2, 3), ARRAY(1))")
        assert dbe.eval("CONTAINS_ALL(ARRAY(1, 2, 3), ARRAY(1, 2))")
        assert dbe.eval("CONTAINS_ALL(ARRAY(1, 2, 3), ARRAY(1, 1, 1, 1, 1, 2))")
        assert dbe.eval("CONTAINS_ALL(ARRAY(1, 2, 3, NULL), ARRAY(1, 2))")
        assert dbe.eval("CONTAINS_ALL(ARRAY(1, 2, 3, 1, 2), ARRAY(1, 2))")
        assert dbe.eval("CONTAINS_ALL(ARRAY(1, 2), ARRAY(1, 2))")
        assert dbe.eval("CONTAINS_ALL(ARRAY(1, 2, 1, 2), ARRAY(1, 2))")
        assert not dbe.eval("CONTAINS_ALL(ARRAY(1, 2), ARRAY(1, 3))")
        assert not dbe.eval("CONTAINS_ALL(ARRAY(1, 2), ARRAY(10, 3))")
        assert not dbe.eval("CONTAINS_ALL(ARRAY(1, 2), ARRAY(1, 1, 3))")
        assert not dbe.eval("CONTAINS_ALL(ARRAY(1, 2), ARRAY(3))")
        # null
        assert not dbe.eval("CONTAINS_ALL(ARRAY(1, 2), ARRAY(1, NULL))")
        assert dbe.eval("CONTAINS_ALL(ARRAY(1, 2, NULL), ARRAY(1, NULL))")

    @pytest.mark.xfail(reason="BI-6163, BI-6165")
    def test_array_contains_all_int_null(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        # null array values
        assert dbe.eval("CONTAINS_ALL([arr_int_null_value], ARRAY(1, NULL))", from_=data_table) is None
        assert not dbe.eval("CONTAINS_ALL(ARRAY(1, 2), [arr_int_null_value])", from_=data_table)

    def test_array_contains_all_float(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval("CONTAINS_ALL(ARRAY(1, 2.9, 3), ARRAY(2.9))")
        assert dbe.eval("CONTAINS_ALL(ARRAY(1, 2.9, 3), ARRAY(1, 2.9))")
        assert dbe.eval("CONTAINS_ALL(ARRAY(1, 2.9, 3), ARRAY(1, 2.9, 2.9, 2.9, 2.9, 2.9))")
        assert dbe.eval("CONTAINS_ALL(ARRAY(1, 2.9, 3, NULL), ARRAY(1, 2.9))")
        assert dbe.eval("CONTAINS_ALL(ARRAY(1, 2.9, 3, 1, 2.9), ARRAY(1, 2.9))")
        assert dbe.eval("CONTAINS_ALL(ARRAY(1, 2.9), ARRAY(1, 2.9))")
        assert dbe.eval("CONTAINS_ALL(ARRAY(1, 2.9, 1, 2.9), ARRAY(1, 2.9))")
        assert not dbe.eval("CONTAINS_ALL(ARRAY(1, 2.9), ARRAY(1, 3.3))")
        assert not dbe.eval("CONTAINS_ALL(ARRAY(1, 2.9), ARRAY(1.1, 3))")
        assert not dbe.eval("CONTAINS_ALL(ARRAY(1, 2.9), ARRAY(1, 1, 3.3))")
        assert not dbe.eval("CONTAINS_ALL(ARRAY(1, 2.9), ARRAY(3.3))")
        # null
        assert not dbe.eval("CONTAINS_ALL(ARRAY(1.1, 2.9), ARRAY(1.1, NULL))")
        assert not dbe.eval("CONTAINS_ALL(ARRAY(1, 2.9), ARRAY(2.9, NULL))")
        assert dbe.eval("CONTAINS_ALL(ARRAY(1.1, 2.9, NULL), ARRAY(1.1, NULL))")
        assert dbe.eval("CONTAINS_ALL(ARRAY(1, 2.9, NULL), ARRAY(2.9, NULL))")

    @pytest.mark.xfail(reason="BI-6163, BI-6165")
    def test_array_contains_all_float_null(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        # null array values
        assert (
            dbe.eval(
                f"CONTAINS_ALL({('[arr_float_null_value]')}, {self._float_array_cast('ARRAY(1.2, NULL)')})",
                from_=data_table,
            )
            is None
        )
        assert not dbe.eval(
            f"CONTAINS_ALL({self._float_array_cast('ARRAY(1.2, 2.3)')}, {self._float_array_cast('[arr_float_null_value]')})",
            from_=data_table,
        )

    def test_array_contains_all_str(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval('CONTAINS_ALL(ARRAY("a", "bc", "potato"), ARRAY("a"))')
        assert dbe.eval('CONTAINS_ALL(ARRAY("a", "bc", "potato"), ARRAY("a", "potato"))')
        assert dbe.eval('CONTAINS_ALL(ARRAY("a", "bc", "potato", NULL), ARRAY("a", "potato"))')
        assert dbe.eval('CONTAINS_ALL(ARRAY("a", "bc", "potato", NULL), ARRAY("a", "a", "a", "a", "a", "potato"))')
        assert dbe.eval('CONTAINS_ALL(ARRAY("a", "bc", "potato", "a", "potato"), ARRAY("a", "potato"))')
        assert dbe.eval('CONTAINS_ALL(ARRAY("a", "potato"), ARRAY("a", "potato"))')
        assert dbe.eval('CONTAINS_ALL(ARRAY("a", "potato", "a", "potato"), ARRAY("a", "potato"))')
        assert not dbe.eval('CONTAINS_ALL(ARRAY("a", "potato"), ARRAY("a", "cheese"))')
        assert not dbe.eval('CONTAINS_ALL(ARRAY("a", "potato"), ARRAY("with", "cheese"))')
        assert not dbe.eval('CONTAINS_ALL(ARRAY("a", "potato"), ARRAY("a", "a", "cheese"))')
        assert not dbe.eval('CONTAINS_ALL(ARRAY("a", "potato"), ARRAY("cheese"))')
        # case-sensitive
        assert not dbe.eval('CONTAINS_ALL(ARRAY("a", "potato", "CHEESE"), ARRAY("cheese"))')
        assert not dbe.eval('CONTAINS_ALL(ARRAY("a", "potato", "banana"), ARRAY("BANANA"))')
        # null
        assert not dbe.eval('CONTAINS_ALL(ARRAY("a", "potato"), ARRAY("a", NULL))')
        assert dbe.eval('CONTAINS_ALL(ARRAY("a", "potato", NULL), ARRAY("a", NULL))')

    @pytest.mark.xfail(reason="BI-6163, BI-6165")
    def test_array_contains_all_str_null(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        # null array values
        assert (
            dbe.eval(
                f"""CONTAINS_ALL({self._str_array_cast('[arr_str_null_value]')}, {self._str_array_cast('ARRAY("cat", NULL)')})""",
                from_=data_table,
            )
            is None
        )
        assert not dbe.eval(
            f"""CONTAINS_ALL({self._str_array_cast('ARRAY("cat", NULL)')}, {self._str_array_cast('[arr_str_null_value]')})""",
            from_=data_table,
        )

    def test_array_contains_all_column(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval("CONTAINS_ALL([arr_int_value], ARRAY(23, 456))", from_=data_table)
        assert not dbe.eval("CONTAINS_ALL([arr_int_value], ARRAY(24, 456))", from_=data_table)

        assert dbe.eval("CONTAINS_ALL([arr_int_value], SLICE([arr_int_value], 2, 2))", from_=data_table)
        assert dbe.eval("CONTAINS_ALL(ARRAY(0, 23, 456, NULL, 123), [arr_int_value])", from_=data_table)

    def test_array_contains_all_string_array(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval('CONTAINS_ALL([arr_str_value], ARRAY("cde"))', from_=data_table)
        assert dbe.eval('CONTAINS_ALL([arr_str_value], ARRAY("cde", NULL))', from_=data_table)
        assert not dbe.eval('CONTAINS_ALL(ARRAY("cde"), [arr_str_value])', from_=data_table)
        assert dbe.eval('CONTAINS_ALL(ARRAY("abc", "cde"), ARRAY("cde"))')
        assert not dbe.eval('CONTAINS_ALL(ARRAY("cde", "abc"), ARRAY("cde", NULL))')

    def test_array_contains_any_int(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval("CONTAINS_ANY(ARRAY(1, 2, 3), ARRAY(1))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(1, 2, 3), ARRAY(1, 2))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(1, 2, 3), ARRAY(1, 5))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(1, 2, 3), ARRAY(1, 1, 1, 1, 1, 5))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(1, 2, 3), ARRAY(1, 1, 1, 1, 1, 2))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(1, 2), ARRAY(1, 2))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(1, 2, 1, 2), ARRAY(1, 2))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(1, 2, 3, NULL), ARRAY(1, 5))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(1, 2, 3), ARRAY(1, 5, NULL))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(1, 2, 3, NULL), ARRAY(1, 5, NULL))")
        assert not dbe.eval("CONTAINS_ANY(ARRAY(1, 2, 3), ARRAY(9, 5))")
        assert not dbe.eval("CONTAINS_ANY(ARRAY(1, 2, 3, NULL), ARRAY(9, 5))")
        assert not dbe.eval("CONTAINS_ANY(ARRAY(1, 2, 3), ARRAY(9, 5, NULL))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(1, 2, 3, NULL), ARRAY(9, 5, NULL))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(1), ARRAY(1))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(1, 1), ARRAY(1))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(1), ARRAY(1, 1))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(1, 1), ARRAY(1, 1))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(1), ARRAY(1, NULL))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(1, 1), ARRAY(1, NULL))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(1), ARRAY(1, 1, NULL))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(1, 1), ARRAY(1, 1, NULL))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(1, NULL), ARRAY(1))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(1, 1, NULL), ARRAY(1))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(1, NULL), ARRAY(1, 1))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(1, 1, NULL), ARRAY(1, 1))")

    @pytest.mark.xfail(reason="BI-6163, BI-6165")
    def test_array_contains_any_int_null(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        # null array values
        assert dbe.eval("CONTAINS_ANY([arr_int_null_value], ARRAY(1, NULL))", from_=data_table) is None
        assert not dbe.eval("CONTAINS_ANY(ARRAY(1, NULL), [arr_int_null_value])", from_=data_table)

    def test_array_contains_any_float(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval("CONTAINS_ANY(ARRAY(0.1, 2.9, 3), ARRAY(0.1))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(0.1, 2.9, 3), ARRAY(0.1, 2.9))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(0.1, 2.9, 3), ARRAY(0.1, 5))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(0.1, 2.9, 3), ARRAY(0.1, 0.1, 0.1, 0.1, 0.1, 5))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(0.1, 2.9, 3), ARRAY(0.1, 0.1, 0.1, 0.1, 0.1, 2.9))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(0.1, 2.9), ARRAY(0.1, 2.9))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(0.1, 2.9, 0.1, 2.9), ARRAY(0.1, 2.9))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(0.1, 2.9, 3, NULL), ARRAY(0.1, 5))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(0.1, 2.9, 3), ARRAY(0.1, 5, NULL))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(0.1, 2.9, 3, NULL), ARRAY(0.1, 5, NULL))")
        assert not dbe.eval("CONTAINS_ANY(ARRAY(0.1, 2.9, 3), ARRAY(9, 5.5))")
        assert not dbe.eval("CONTAINS_ANY(ARRAY(0.1, 2.9, 3, NULL), ARRAY(9, 5.5))")
        assert not dbe.eval("CONTAINS_ANY(ARRAY(0.1, 2.9, 3), ARRAY(9.85, 5, NULL))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(0.1, 2.9, 3, NULL), ARRAY(9, 5.5, NULL))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(0.1), ARRAY(0.1))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(0.1, 0.1), ARRAY(0.1))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(0.1), ARRAY(0.1, 0.1))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(0.1, 0.1), ARRAY(0.1, 0.1))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(0.1), ARRAY(0.1, NULL))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(0.1, 0.1), ARRAY(0.1, NULL))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(0.1), ARRAY(0.1, 0.1, NULL))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(0.1, 0.1), ARRAY(0.1, 0.1, NULL))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(0.1, NULL), ARRAY(0.1))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(0.1, 0.1, NULL), ARRAY(0.1))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(0.1, NULL), ARRAY(0.1, 0.1))")
        assert dbe.eval("CONTAINS_ANY(ARRAY(0.1, 0.1, NULL), ARRAY(0.1, 0.1))")

    @pytest.mark.xfail(reason="BI-6163, BI-6165")
    def test_array_contains_any_float_null(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        # null array values
        assert (
            dbe.eval(
                f"CONTAINS_ANY({self._float_array_cast('[arr_float_null_value]')}, {self._float_array_cast('ARRAY(1.2, NULL)')})",
                from_=data_table,
            )
            is None
        )
        assert not dbe.eval(
            f"CONTAINS_ANY({self._float_array_cast('ARRAY(1.2, NULL)')}, {self._float_array_cast('[arr_float_null_value]')})",
            from_=data_table,
        )

    def test_array_contains_any_str(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval('CONTAINS_ANY(ARRAY("foo", "bar", "potato"), ARRAY("foo"))')
        assert dbe.eval('CONTAINS_ANY(ARRAY("foo", "bar", "potato"), ARRAY("foo", "potato"))')
        assert dbe.eval('CONTAINS_ANY(ARRAY("foo", "bar", "potato"), ARRAY("foo", "tomato"))')
        assert dbe.eval(
            'CONTAINS_ANY(ARRAY("foo", "bar", "potato"), ARRAY("foo", "foo", "foo", "foo", "foo", "tomato"))',
            from_=data_table,
        )
        assert dbe.eval(
            'CONTAINS_ANY(ARRAY("foo", "bar", "potato"), ARRAY("foo", "foo", "foo", "foo", "foo", "potato"))',
            from_=data_table,
        )
        assert dbe.eval('CONTAINS_ANY(ARRAY("foo", "bar"), ARRAY("foo", "bar"))')
        assert dbe.eval('CONTAINS_ANY(ARRAY("foo", "bar", NULL), ARRAY("foo", "tomato"))')
        assert dbe.eval('CONTAINS_ANY(ARRAY("foo", "bar", NULL), ARRAY("foo", "tomato", NULL))')
        assert not dbe.eval('CONTAINS_ANY(ARRAY("foo", "bar", "potato"), ARRAY("tomato", "cheese"))')
        assert not dbe.eval('CONTAINS_ANY(ARRAY("foo", "bar", "potato", NULL), ARRAY("tomato", "cheese"))')
        assert not dbe.eval('CONTAINS_ANY(ARRAY("foo", "bar", "potato"), ARRAY("tomato", "cheese", NULL))')
        assert dbe.eval('CONTAINS_ANY(ARRAY("foo", "bar", "potato", NULL), ARRAY("banana", "tomato", NULL))')
        assert dbe.eval('CONTAINS_ANY(ARRAY("foo"), ARRAY("foo"))')
        assert dbe.eval('CONTAINS_ANY(ARRAY("foo", "foo"), ARRAY("foo"))')
        assert dbe.eval('CONTAINS_ANY(ARRAY("foo"), ARRAY("foo", "foo"))')
        assert dbe.eval('CONTAINS_ANY(ARRAY("foo", "foo"), ARRAY("foo", "foo"))')
        assert dbe.eval('CONTAINS_ANY(ARRAY("foo"), ARRAY("foo", NULL))')
        assert dbe.eval('CONTAINS_ANY(ARRAY("foo", "foo"), ARRAY("foo", NULL))')
        assert dbe.eval('CONTAINS_ANY(ARRAY("foo"), ARRAY("foo", "foo", NULL))')
        assert dbe.eval('CONTAINS_ANY(ARRAY("foo", "foo"), ARRAY("foo", "foo", NULL))')
        assert dbe.eval('CONTAINS_ANY(ARRAY("foo", NULL), ARRAY("foo"))')
        assert dbe.eval('CONTAINS_ANY(ARRAY("foo", "foo", NULL), ARRAY("foo"))')
        assert dbe.eval('CONTAINS_ANY(ARRAY("foo", NULL), ARRAY("foo", "foo"))')
        assert dbe.eval('CONTAINS_ANY(ARRAY("foo", "foo", NULL), ARRAY("foo", "foo"))')
        # case-sensitive
        assert not dbe.eval('CONTAINS_ANY(ARRAY("foo", "foo", "CHEESE"), ARRAY("banana", "cheese"))')
        assert not dbe.eval('CONTAINS_ANY(ARRAY("foo", "foo", "cheese"), ARRAY("banana", "CHEESE"))')
        # "null"
        assert not dbe.eval('CONTAINS_ANY(ARRAY("foo", "foo", "NULL"), ARRAY("banana", NULL))')
        assert not dbe.eval('CONTAINS_ANY(ARRAY("foo", "foo", NULL), ARRAY("banana", "NULL"))')

    @pytest.mark.xfail(reason="BI-6163, BI-6165")
    def test_array_contains_any_str_null(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        # null array values
        assert (
            dbe.eval(
                f"""CONTAINS_ANY({self._str_array_cast('[arr_str_null_value]')}, {self._str_array_cast('ARRAY("cat", NULL)')})""",
                from_=data_table,
            )
            is None
        )
        assert not dbe.eval(
            f"""CONTAINS_ANY({self._str_array_cast('ARRAY("cat", NULL)')}, {self._str_array_cast('[arr_str_null_value]')})""",
            from_=data_table,
        )

    def test_array_contains_any_column(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval("CONTAINS_ANY([arr_int_value], ARRAY(24, 456))", from_=data_table)
        assert not dbe.eval("CONTAINS_ANY([arr_int_value], ARRAY(24, 457))", from_=data_table)

        assert dbe.eval(
            f"CONTAINS_ANY([arr_float_value], {self._float_array_cast('ARRAY(19, 0.123)')})", from_=data_table
        )
        assert not dbe.eval(
            f"CONTAINS_ANY({self._float_array_cast('[arr_float_value]')}, {self._float_array_cast('ARRAY(190, 0.0123)')})",
            from_=data_table,
        )

        assert dbe.eval("CONTAINS_ANY([arr_int_value], SLICE([arr_int_value], 2, 1))", from_=data_table)
        assert dbe.eval("CONTAINS_ANY(ARRAY(0, 23, 456, NULL, 123), [arr_int_value])", from_=data_table)

    def test_array_contains_any_string_array(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval('CONTAINS_ANY([arr_str_value], ARRAY("cde"))', from_=data_table)
        assert dbe.eval('CONTAINS_ANY([arr_str_value], ARRAY("123", NULL))', from_=data_table)
        assert dbe.eval('CONTAINS_ANY(ARRAY("cde"), [arr_str_value])', from_=data_table)

    def test_array_not_contains_int(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert not dbe.eval("NOTCONTAINS(ARRAY(1, 2, 3), 1)")
        assert dbe.eval("NOTCONTAINS(ARRAY(1, 2, 3), 5)")
        assert dbe.eval("NOTCONTAINS(ARRAY(1, 2, 3, NULL), 5)")
        assert dbe.eval("NOTCONTAINS(ARRAY(1, 2, 3), NULL)")
        assert not dbe.eval("NOTCONTAINS(ARRAY(1, 2, 3, NULL), NULL)")
        assert not dbe.eval("NOTCONTAINS(ARRAY(1), 1)")
        assert not dbe.eval("NOTCONTAINS(ARRAY(1, 1), 1)")

    def test_array_not_contains_float(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert not dbe.eval("NOTCONTAINS(ARRAY(1.1, 2.9, 3), 1.1)")
        assert not dbe.eval("NOTCONTAINS(ARRAY(1.1, 2.9, 3, NULL), 1.1)")
        assert dbe.eval("NOTCONTAINS(ARRAY(1.1, 2.9, 3), 3.7)")
        assert dbe.eval("NOTCONTAINS(ARRAY(1.1, 2.9, 3, NULL), 3.7)")
        assert dbe.eval("NOTCONTAINS(ARRAY(1.1, 2.9, 3), NULL)")
        assert not dbe.eval("NOTCONTAINS(ARRAY(1.1, 2.9, 3, NULL), NULL)")
        assert not dbe.eval("NOTCONTAINS(ARRAY(1.1), 1.1)")
        assert not dbe.eval("NOTCONTAINS(ARRAY(1.1, 1.1), 1.1)")

    def test_array_not_contains_str(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert not dbe.eval('NOTCONTAINS(ARRAY("foo", "bar", "potato"), "potato")')
        assert not dbe.eval('NOTCONTAINS(ARRAY("foo", "bar", "potato", NULL), "potato")')
        assert dbe.eval('NOTCONTAINS(ARRAY("foo", "bar", "potato"), "tomato")')
        assert dbe.eval('NOTCONTAINS(ARRAY("foo", "bar", "potato", NULL), "tomato")')
        assert dbe.eval('NOTCONTAINS(ARRAY("foo", "bar", "potato", NULL), "tomato")')
        assert dbe.eval('NOTCONTAINS(ARRAY("foo", "bar", "potato"), NULL)')
        assert not dbe.eval('NOTCONTAINS(ARRAY("foo", "bar", "potato", NULL), NULL)')
        assert not dbe.eval('NOTCONTAINS(ARRAY("foo"), "foo")')
        assert not dbe.eval('NOTCONTAINS(ARRAY("foo", "foo"), "foo")')
        # case-sensitive
        assert dbe.eval('NOTCONTAINS(ARRAY("foo", "bar", "potato"), "POTATO")')
        assert dbe.eval('NOTCONTAINS(ARRAY("foo", "bar", "CHEESE"), "cheese")')
        # "null"
        assert dbe.eval('NOTCONTAINS(ARRAY("foo", "bar", "NULL"), NULL)')
        assert dbe.eval('NOTCONTAINS(ARRAY("foo", "bar", NULL), "NULL")')

    def test_array_not_contains_column(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert not dbe.eval("NOTCONTAINS([arr_int_value], 23)", from_=data_table)
        assert dbe.eval("NOTCONTAINS([arr_int_value], 24)", from_=data_table)
        assert not dbe.eval('NOTCONTAINS([arr_str_value], "cde")', from_=data_table)
        assert not dbe.eval("NOTCONTAINS([arr_str_value], NULL)", from_=data_table)

        assert not dbe.eval("NOTCONTAINS([arr_float_value], NULL)", from_=data_table)
        assert dbe.eval("NOTCONTAINS([arr_float_value], 76.1)", from_=data_table)

        assert not dbe.eval("NOTCONTAINS([arr_int_value], NULL)", from_=data_table)
        assert dbe.eval("NOTCONTAINS([arr_int_value], 931)", from_=data_table)

        assert not dbe.eval("NOTCONTAINS([arr_str_value], GET_ITEM([arr_str_value], 1))", from_=data_table)
        assert not dbe.eval("NOTCONTAINS([arr_str_value], GET_ITEM([arr_str_value], 2))", from_=data_table)
        assert not dbe.eval("NOTCONTAINS([arr_str_value], GET_ITEM([arr_str_value], 3))", from_=data_table)
        assert not dbe.eval("NOTCONTAINS([arr_str_value], GET_ITEM([arr_str_value], 4))", from_=data_table)
        assert not dbe.eval("NOTCONTAINS([arr_str_value], GET_ITEM([arr_str_value], 4))", from_=data_table)

    def test_array_slice(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval("SLICE([arr_int_value], 2, 2)", from_=data_table) == dbe.eval("ARRAY(23, 456)")
        assert dbe.eval("SLICE(ARRAY(1, 2, 3, 4), 2, 2)") == dbe.eval("ARRAY(2, 3)")
        assert dbe.eval("SLICE(ARRAY(1, 2, 3, 4), 2, 1)") == dbe.eval("ARRAY(2)")
        assert str(dbe.eval("SLICE(ARRAY(1, 2, 3, 4), 2, 0)")) == "[]"

    def test_replace_array_int(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval("REPLACE(ARRAY(1), 1, 7)") == dbe.eval("ARRAY(7)")
        assert dbe.eval("REPLACE(ARRAY(1), 2, 7)") == dbe.eval("ARRAY(1)")
        assert dbe.eval("REPLACE(ARRAY(1, 1, 1), 1, 7)") == dbe.eval("ARRAY(7, 7, 7)")
        assert dbe.eval("REPLACE(ARRAY(1, 1, 1), 2, 7)") == dbe.eval("ARRAY(1, 1, 1)")
        assert dbe.eval("REPLACE(ARRAY(1, 2, 3), 1, 7)") == dbe.eval("ARRAY(7, 2, 3)")
        assert dbe.eval("REPLACE(ARRAY(1, 1, 2, 3), 1, 7)") == dbe.eval("ARRAY(7, 7, 2, 3)")
        assert dbe.eval("REPLACE(ARRAY(1, 2, 3), 6, 7)") == dbe.eval("ARRAY(1, 2, 3)")
        assert dbe.eval("REPLACE(ARRAY(1), 1, 1)") == dbe.eval("ARRAY(1)")
        assert dbe.eval("REPLACE(ARRAY(1, 2, 3), 1, 1)") == dbe.eval("ARRAY(1, 2, 3)")
        # null
        assert dbe.eval("REPLACE(ARRAY(1, 2, 3, NULL), NULL, 9)") == dbe.eval("ARRAY(1, 2, 3, 9)")

    def test_replace_array_float(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval(self._float_array_cast("REPLACE(ARRAY(1.1), 1.1, 7.9)")) == dbe.eval(
            self._float_array_cast("ARRAY(7.9)")
        )
        assert dbe.eval(self._float_array_cast("REPLACE(ARRAY(1.1), 2.2, 7.9)")) == dbe.eval(
            self._float_array_cast("ARRAY(1.1)")
        )
        assert dbe.eval(self._float_array_cast("REPLACE(ARRAY(1.1, 1.1, 1.1), 1.1, 7.9)")) == dbe.eval(
            self._float_array_cast("ARRAY(7.9, 7.9, 7.9)")
        )
        assert dbe.eval(self._float_array_cast("REPLACE(ARRAY(1.1, 1.1, 1.1), 2.2, 7.9)")) == dbe.eval(
            self._float_array_cast("ARRAY(1.1, 1.1, 1.1)")
        )
        assert dbe.eval(self._float_array_cast("REPLACE(ARRAY(1.1, 2.2, 3.3), 1.1, 7.9)")) == dbe.eval(
            self._float_array_cast("ARRAY(7.9, 2.2, 3.3)")
        )
        assert dbe.eval(self._float_array_cast("REPLACE(ARRAY(1.1, 1.1, 2.2, 3.3), 1.1, 7.9)")) == dbe.eval(
            self._float_array_cast("ARRAY(7.9, 7.9, 2.2, 3.3)")
        )
        assert dbe.eval(self._float_array_cast("REPLACE(ARRAY(1.1, 2.2, 3.3), 6.6, 7.9)")) == dbe.eval(
            self._float_array_cast("ARRAY(1.1, 2.2, 3.3)")
        )
        assert dbe.eval(self._float_array_cast("REPLACE(ARRAY(1.1), 1.1, 1.1)")) == dbe.eval(
            self._float_array_cast("ARRAY(1.1)")
        )
        assert dbe.eval(self._float_array_cast("REPLACE(ARRAY(1.1, 2.2, 3.3), 1.1, 1.1)")) == dbe.eval(
            self._float_array_cast("ARRAY(1.1, 2.2, 3.3)")
        )
        # null
        assert dbe.eval(self._float_array_cast("REPLACE(ARRAY(1.1, 2.2, 3.3, NULL), NULL, 9.9)")) == dbe.eval(
            self._float_array_cast("ARRAY(1.1, 2.2, 3.3, 9.9)")
        )

    def test_replace_array_str(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval('REPLACE(ARRAY("cheese"), "cheese", "burger")') == dbe.eval('ARRAY("burger")')
        assert dbe.eval('REPLACE(ARRAY("cheese"), "potato", "burger")') == dbe.eval('ARRAY("cheese")')
        assert dbe.eval('REPLACE(ARRAY("cheese", "cheese", "cheese"), "cheese", "burger")') == dbe.eval(
            'ARRAY("burger", "burger", "burger")'
        )
        assert dbe.eval('REPLACE(ARRAY("cheese", "cheese", "cheese"), "potato", "burger")') == dbe.eval(
            'ARRAY("cheese", "cheese", "cheese")'
        )
        assert dbe.eval('REPLACE(ARRAY("cheese", "burger", "cat"), "cheese", "bread")') == dbe.eval(
            'ARRAY("bread", "burger", "cat")'
        )
        assert dbe.eval('REPLACE(ARRAY("cheese", "cheese", "burger", "cat"), "cheese", "bread")') == dbe.eval(
            'ARRAY("bread", "bread", "burger", "cat")'
        )
        assert dbe.eval('REPLACE(ARRAY("cheese", "burger", "cat"), "salad", "potato")') == dbe.eval(
            'ARRAY("cheese", "burger", "cat")'
        )
        assert dbe.eval('REPLACE(ARRAY("cheese"), "cheese", "cheese")') == dbe.eval('ARRAY("cheese")')
        assert dbe.eval('REPLACE(ARRAY("cheese", "burger", "cat"), "cheese", "cheese")') == dbe.eval(
            'ARRAY("cheese", "burger", "cat")'
        )
        # null
        assert dbe.eval('REPLACE(ARRAY("cheese", "burger", "cat", NULL), NULL, "bread")') == dbe.eval(
            'ARRAY("cheese", "burger", "cat", "bread")'
        )
        # "null"
        assert dbe.eval('REPLACE(ARRAY("cheese", "burger", "cat", "NULL"), NULL, "bread")') == dbe.eval(
            'ARRAY("cheese", "burger", "cat", "NULL")'
        )
        assert dbe.eval('REPLACE(ARRAY("cheese", "burger", "cat", NULL), "NULL", "bread")') == dbe.eval(
            'ARRAY("cheese", "burger", "cat", NULL)'
        )

    def test_replace_array_column(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval("REPLACE([arr_int_value], -2, 1000)", from_=data_table) == dbe.eval("ARRAY(0, 23, 456, NULL)")
        assert dbe.eval("REPLACE([arr_int_value], 456, 1000)", from_=data_table) == dbe.eval("ARRAY(0, 23, 1000, NULL)")
        assert dbe.eval("REPLACE(ARRAY(NULL, NULL, 1), NULL, 1)") == dbe.eval("ARRAY(1, 1, 1)")
        assert dbe.eval('REPLACE(ARRAY("NULL", NULL, "NULL"), "NULL", "LUL")') == dbe.eval('ARRAY("LUL", NULL, "LUL")')
        assert dbe.eval("REPLACE(ARRAY(45, 1, 45), GET_ITEM(ARRAY(24, 45), 2), 22 + 2)") == dbe.eval("ARRAY(24, 1, 24)")

    def test_array_cast(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval("CAST_ARR_INT([arr_float_value])", from_=data_table) == dbe.eval("ARRAY(0, 45, 0, NULL)")
        assert dbe.eval("CAST_ARR_INT([arr_int_value])", from_=data_table) == dbe.eval(
            "[arr_int_value]", from_=data_table
        )
        assert dbe.eval('CAST_ARR_INT(ARRAY("1", "3", "2", NULL))') == dbe.eval("ARRAY(1, 3, 2, NULL)")

        assert dbe.eval("CAST_ARR_FLOAT([arr_float_value])", from_=data_table) == dbe.eval(
            "[arr_float_value]", from_=data_table
        )
        assert dbe.eval("CAST_ARR_FLOAT([arr_int_value])", from_=data_table) == dbe.eval("ARRAY(0, 23.0, 456, NULL)")
        assert dbe.eval('CAST_ARR_FLOAT(ARRAY("1", "3.3", "2", NULL))') == dbe.eval("ARRAY(1, 3.3, 2, NULL)")

        assert dbe.eval("CAST_ARR_STR([arr_float_value])", from_=data_table) in (
            dbe.eval('ARRAY("0.0", "45.0", "0.123", NULL)'),
            dbe.eval('ARRAY("0", "45", "0.123", NULL)'),
            dbe.eval('ARRAY("0.0", "45.0", "0.123", NULL)'),
            dbe.eval('ARRAY("0", "45", "0.123", NULL)'),
        )
        assert dbe.eval("CAST_ARR_STR([arr_int_value])", from_=data_table) == dbe.eval('ARRAY("0", "23", "456", NULL)')
        assert dbe.eval("CAST_ARR_STR([arr_str_value])", from_=data_table) == dbe.eval(
            "[arr_str_value]", from_=data_table
        )

    def test_array_get_item_nested(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        src = "ARRAY('qwe', 'rty', 'uio')"

        assert dbe.eval(f"GET_ITEM({src}, 1)") == "qwe"
        assert dbe.eval("GET_ITEM([arr_int_value], 3)", from_=data_table) == 456

        assert dbe.eval(f"GET_ITEM(SLICE({src}, 2, 1), 1)") == "rty"
        assert dbe.eval("GET_ITEM(SLICE([arr_int_value], 2, 2), 2)", from_=data_table) == 456

        assert dbe.eval(f"SLICE(SLICE({src}, 2, 1), 1, 1)") == dbe.eval('ARRAY("rty")')
        assert dbe.eval("SLICE(SLICE([arr_int_value], 2, 1), 1, 1)", from_=data_table) == dbe.eval("ARRAY(23)")

        assert dbe.eval("STARTSWITH(SLICE(ARRAY(1, 2, 3, 4, 5), 1, 4), SLICE(ARRAY(1, 2, 3), 1, 2))")
        assert not dbe.eval("STARTSWITH(SLICE([arr_int_value], 1, 4), ARRAY(1, 2, 3))", from_=data_table)
        assert dbe.eval("STARTSWITH(SLICE([arr_int_value], 1, 3), ARRAY(0, 23, 456))", from_=data_table)

    def test_startswith_number_array(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval("STARTSWITH(ARRAY(1, 2, 3, 4, 5), ARRAY(1, 2, 3))")
        assert dbe.eval("STARTSWITH(ARRAY(1, 2, 3, 4, 5), ARRAY(1, 2, 3, 4, 5))")
        assert dbe.eval("STARTSWITH(ARRAY(1, NULL, 3, NULL, 5), ARRAY(1, NULL, 3, NULL))")
        assert not dbe.eval("STARTSWITH(ARRAY(1, 2, 3, 4, 5), ARRAY(1, 2, 3, 4, 5, 6, 7))")
        assert not dbe.eval("STARTSWITH(ARRAY(1, 2, 3, 4, 5), ARRAY(5, 4, 3, 2, 1))")
        assert not dbe.eval("STARTSWITH(ARRAY(1, 2, 3, 4, 5), ARRAY(3, 4))")
        assert not dbe.eval("STARTSWITH(ARRAY(1, 2, NULL, 4, 5), ARRAY(2, NULL))")
        assert dbe.eval("STARTSWITH(ARRAY(1.1, NULL, 3.3, 4.4, 5.5), ARRAY(1.1, NULL, 3.3))")
        assert dbe.eval('STARTSWITH(ARRAY("", "NULL", NULL, "12"), ARRAY("", "NULL"))')
        assert dbe.eval("STARTSWITH(ARRAY(1, 2, 3, 4, 5), ARRAY(1, 2, 3))")
        assert dbe.eval("STARTSWITH(ARRAY(1, 2, 3, 4, 5), ARRAY(1, 2, 3, 4, 5))")
        assert dbe.eval("STARTSWITH(ARRAY(1, NULL, 3, NULL, 5), ARRAY(1, NULL, 3, NULL))")
        assert not dbe.eval("STARTSWITH(ARRAY(1, 2, 3, 4, 5), ARRAY(1, 2, 3, 4, 5, 6, 7))")
        assert not dbe.eval("STARTSWITH(ARRAY(1, 2, 3, 4, 5), ARRAY(5, 4, 3, 2, 1))")
        assert not dbe.eval("STARTSWITH(ARRAY(1, 2, 3, 4, 5), ARRAY(3, 4))")
        assert not dbe.eval("STARTSWITH(ARRAY(1, 2, NULL, 4, 5), ARRAY(2, NULL))")
        assert dbe.eval("STARTSWITH(ARRAY(1.1, NULL, 3.3, 4.4, 5.5), ARRAY(1.1, NULL, 3.3))")
        assert dbe.eval('STARTSWITH(ARRAY("", "NULL", NULL, "12"), ARRAY("", "NULL"))')

        assert dbe.eval("STARTSWITH([arr_int_value], [arr_int_value])", from_=data_table)
        assert dbe.eval("STARTSWITH([arr_float_value], [arr_float_value])", from_=data_table)

    @abc.abstractmethod
    def test_startswith_string_array(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        pass  # Defined in a dialect-specific way

    def test_array_remove(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        # with ARRAY func | remove literal not NULL
        assert dbe.eval("ARR_REMOVE(ARRAY(1, 2, NULL), 1)") == dbe.eval("ARRAY(2, NULL)")
        assert dbe.eval('ARR_REMOVE(ARRAY("a", "b", NULL), "a")') == dbe.eval('ARRAY("b", NULL)')
        assert dbe.eval("ARR_REMOVE(ARRAY(1, 2, NULL), 1)") == dbe.eval("ARRAY(2, NULL)")
        assert dbe.eval('ARR_REMOVE(ARRAY("a", "b", NULL), "a")') == dbe.eval('ARRAY("b", NULL)')
        if self.make_decimal_cast:
            assert dbe.eval("ARR_REMOVE(ARRAY(1.1, 2.2, NULL), 1.1)") == dbe.eval(
                f'ARRAY(DB_CAST(2.2, "{self.make_decimal_cast}", 2, 1), NULL)'
            )
        else:
            assert dbe.eval("ARR_REMOVE(ARRAY(1.1, 2.2, NULL), 1.1)") == dbe.eval("ARRAY(2.2, NULL)")
            assert dbe.eval("ARR_REMOVE(ARRAY(1.1, 2.2, NULL), 1.1)") == dbe.eval("ARRAY(2.2, NULL)")

        # with ARRAY func | remove literal NULL
        assert dbe.eval("ARR_REMOVE(ARRAY(1, 2, NULL), NULL)") == dbe.eval("ARRAY(1, 2)")
        assert dbe.eval('ARR_REMOVE(ARRAY("a", "b", NULL), NULL)') == dbe.eval('ARRAY("a", "b")')
        assert dbe.eval("ARR_REMOVE(ARRAY(1, 2, NULL), NULL)") == dbe.eval("ARRAY(1, 2)")
        assert dbe.eval('ARR_REMOVE(ARRAY("a", "b", NULL), NULL)') == dbe.eval('ARRAY("a", "b")')
        if self.make_decimal_cast:
            assert dbe.eval("ARR_REMOVE(ARRAY(1.1, 2.2, NULL), NULL)") == dbe.eval(
                f'ARRAY(DB_CAST(1.1, "{self.make_decimal_cast}", 2, 1), DB_CAST(2.2, "{self.make_decimal_cast}", 2, 1))'
            )
        else:
            assert dbe.eval("ARR_REMOVE(ARRAY(1.1, 2.2, NULL), NULL)") == dbe.eval("ARRAY(1.1, 2.2)")
            assert dbe.eval("ARR_REMOVE(ARRAY(1.1, 2.2, NULL), NULL)") == dbe.eval("ARRAY(1.1, 2.2)")

        # with array in DB | remove literal not NULL
        assert dbe.eval("ARR_REMOVE([arr_int_value], 0)", from_=data_table) == dbe.eval("ARRAY(23, 456, NULL)")
        assert dbe.eval('ARR_REMOVE([arr_str_value], "")', from_=data_table) == dbe.eval('ARRAY("cde", NULL)')
        if self.make_float_cast:
            assert dbe.eval(
                f'ARR_REMOVE([arr_float_value], DB_CAST(0.0, "{self.make_float_cast}"))', from_=data_table
            ) == dbe.eval("ARRAY(45, 0.123, NULL)")
        else:
            assert dbe.eval("ARR_REMOVE([arr_float_value], 0.0)", from_=data_table) == dbe.eval(
                "ARRAY(45, 0.123, NULL)"
            )

        # with array in DB | remove literal NULL
        assert dbe.eval("ARR_REMOVE([arr_int_value], NULL)", from_=data_table) == dbe.eval("ARRAY(0, 23, 456)")
        assert dbe.eval("ARR_REMOVE([arr_str_value], NULL)", from_=data_table) == dbe.eval('ARRAY("", "", "cde")')
        assert dbe.eval("ARR_REMOVE([arr_float_value], NULL)", from_=data_table) == dbe.eval("ARRAY(0, 45, 0.123)")

        # with array in DB | remove non const
        assert dbe.eval("ARR_REMOVE([arr_int_value], GET_ITEM([arr_int_value], 1))", from_=data_table) == dbe.eval(
            "ARRAY(23, 456, NULL)"
        )
        assert dbe.eval("ARR_REMOVE([arr_str_value], GET_ITEM([arr_str_value], 1))", from_=data_table) == dbe.eval(
            'ARRAY("cde", NULL)'
        )
        assert dbe.eval("ARR_REMOVE([arr_float_value], GET_ITEM([arr_float_value], 1))", from_=data_table) == dbe.eval(
            "ARRAY(45, 0.123, NULL)"
        )

        # with ARRAY func remove const
        assert dbe.eval("ARR_REMOVE(ARRAY(1, 2), 1)") == dbe.eval("ARRAY(2)")
        assert dbe.eval(self._float_array_cast("ARR_REMOVE(ARRAY(1.1, 2.2), 1.1)")) == dbe.eval(
            self._float_array_cast("ARRAY(2.2)")
        )
        assert dbe.eval("ARR_REMOVE(ARRAY('1', '2'), '1')") == dbe.eval("ARRAY('2')")
        assert dbe.eval("LEN(ARR_REMOVE(ARRAY(1), 1))") == 0
        assert dbe.eval("LEN(ARR_REMOVE(ARRAY(1.1), 1.1))") == 0
        assert dbe.eval('LEN(ARR_REMOVE(ARRAY("a"), "a"))') == 0
        assert dbe.eval("LEN(ARR_REMOVE(ARRAY(1, 1, 1, 1), 1))") == 0
        assert dbe.eval("LEN(ARR_REMOVE(ARRAY(1.1, 1.1, 1.1, 1.1), 1.1))") == 0
        assert dbe.eval('LEN(ARR_REMOVE(ARRAY("a", "a", "a", "a"), "a"))') == 0

    def test_array_intersection(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval("ARR_INTERSECT(ARRAY(1, 2))") in (
            dbe.eval("ARRAY(1, 2)"),
            dbe.eval("ARRAY(2, 1)"),
        )
        assert dbe.eval("ARR_INTERSECT(ARRAY(1, 2), ARRAY(1, 2))") in (
            dbe.eval("ARRAY(1, 2)"),
            dbe.eval("ARRAY(2, 1)"),
        )
        assert dbe.eval("ARR_INTERSECT(ARRAY(1, 2, 2), ARRAY(1, 2, 2))") in (
            dbe.eval("ARRAY(1, 2)"),
            dbe.eval("ARRAY(2, 1)"),
        )
        assert dbe.eval("ARR_INTERSECT(ARRAY(1, 2), ARRAY(3, 4), ARRAY(5, 6))") in ([], "[]")
        assert dbe.eval("ARR_INTERSECT(ARRAY(1, 2), ARRAY(2, 3), ARRAY(3, 4))") in ([], "[]")
        assert dbe.eval("ARR_INTERSECT(ARRAY(1, 2), ARRAY(2), ARRAY(2, 2, 3))") == dbe.eval("ARRAY(2)")
        assert dbe.eval("ARR_INTERSECT(ARRAY(1, 2, 3), ARRAY(2, 3, 4))") in (
            dbe.eval("ARRAY(2, 3)"),
            dbe.eval("ARRAY(3, 2)"),
        )
        assert dbe.eval("ARR_INTERSECT(ARRAY(2, 3), ARRAY(1, 2, 3, 4))") in (
            dbe.eval("ARRAY(2, 3)"),
            dbe.eval("ARRAY(3, 2)"),
        )
        assert dbe.eval("ARR_INTERSECT(ARRAY(2, 3, 2, 2, 4), ARRAY(1, 2, 3, 2), ARRAY(2, 3, 2))") in (
            dbe.eval("ARRAY(2, 3)"),
            dbe.eval("ARRAY(3, 2)"),
        )
        assert dbe.eval("ARR_INTERSECT(ARRAY(1, 2, 3, NULL), ARRAY(2, 3, 4))") in (
            dbe.eval("ARRAY(2, 3)"),
            dbe.eval("ARRAY(3, 2)"),
        )
        assert dbe.eval("ARR_INTERSECT(ARRAY(0, 2, NULL, NULL), ARRAY(0, NULL), ARRAY(2, NULL, 0))") in (
            dbe.eval("ARRAY(0, NULL)"),
            dbe.eval("ARRAY(NULL, 0)"),
        )
        assert dbe.eval("ARR_INTERSECT(ARRAY(0, NULL, NULL), ARRAY(NULL, 0, NULL, NULL))") in (
            dbe.eval("ARRAY(0, NULL)"),
            dbe.eval("ARRAY(NULL, 0)"),
        )

        assert dbe.eval("ARR_INTERSECT(ARRAY(0, 5, 4.999), ARRAY(0, 5.0), ARRAY(4.999))") in (
            [],
            "[]",
        )
        if self.make_decimal_cast:
            assert dbe.eval("ARR_INTERSECT(ARRAY(5, 49.999), ARRAY(5, 49.999))") in (
                dbe.eval(
                    f'ARRAY(DB_CAST(5.0, "{self.make_decimal_cast}", 2, 1), DB_CAST(49.999, "{self.make_decimal_cast}", 5, 3))'
                ),
                dbe.eval(
                    f'ARRAY(DB_CAST(49.999, "{self.make_decimal_cast}", 5, 3), DB_CAST(5.0, "{self.make_decimal_cast}", 2, 1))'
                ),
            )
            assert dbe.eval("ARR_INTERSECT(ARRAY(0, 5, 4.999), ARRAY(0, 5.0))") in (
                dbe.eval(
                    f'ARRAY(DB_CAST(0.0, "{self.make_decimal_cast}", 2, 1), DB_CAST(5.0, "{self.make_decimal_cast}", 2, 1))'
                ),
                dbe.eval(
                    f'ARRAY(DB_CAST(5.0, "{self.make_decimal_cast}", 2, 1), DB_CAST(0.0, "{self.make_decimal_cast}", 2, 1))'
                ),
            )
            assert dbe.eval("ARR_INTERSECT(ARRAY(0, 5, 4.999), ARRAY(4.999))") == dbe.eval(
                f'ARRAY(DB_CAST(4.999, "{self.make_decimal_cast}", 4, 3))'
            )
        else:
            assert dbe.eval("ARR_INTERSECT(ARRAY(5, 49.999), ARRAY(5, 49.999))") in (
                dbe.eval("ARRAY(5.0, 49.999)"),
                dbe.eval("ARRAY(49.999, 5.0)"),
            )
            assert dbe.eval("ARR_INTERSECT(ARRAY(0, 5, 4.999), ARRAY(0, 5.0))") in (
                dbe.eval("ARRAY(0, 5.0)"),
                dbe.eval("ARRAY(5.0, 0)"),
            )
            assert dbe.eval("ARR_INTERSECT(ARRAY(0, 5, 4.999), ARRAY(4.999))") == dbe.eval("ARRAY(4.999)")

        assert dbe.eval('ARR_INTERSECT(ARRAY("a", "b", "c"), ARRAY("abc"))') in ([], "[]")
        assert dbe.eval('ARR_INTERSECT(ARRAY("a", "b", "c"), ARRAY("a", "bc"))') == dbe.eval('ARRAY("a")')
        assert dbe.eval('ARR_INTERSECT(ARRAY("cba"), ARRAY("abc"))') in ([], "[]")
        assert dbe.eval(
            'ARR_INTERSECT(ARRAY("ab", "c", "c"), ARRAY("ab", "b", "c", "c"), ARRAY("a", "c", "c", "ab"))',
            from_=data_table,
        ) in (dbe.eval('ARRAY("ab", "c")'), dbe.eval('ARRAY("c", "ab")'))

    def test_array_distinct_int(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        # Takes array input, returns array with unique values
        result = dbe.eval("ARR_DISTINCT(ARRAY(1, 2, 2, 3, 1, 3))")
        # Should contain unique values: 1, 2, 3 (order may vary)
        assert len(result) == 3
        assert set(result) == {1, 2, 3}

        # Single element array
        assert dbe.eval("ARR_DISTINCT(ARRAY(42))") == [42]

        # All same elements should return single element
        assert dbe.eval("ARR_DISTINCT(ARRAY(5, 5, 5))") == [5]

        # With NULL values
        result = dbe.eval("ARR_DISTINCT(ARRAY(1, NULL, 1, NULL, 2))")
        assert len(result) == 3  # Should have 1, NULL, 2
        assert set(result) == {1, 2, None}

    def test_array_distinct_float(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        # Takes array input, returns array with unique values
        result = dbe.eval(self._float_array_cast("ARR_DISTINCT(ARRAY(1.1, 2.2, 2.2, 3.3, 1.1))"))
        # result = dbe.eval("ARR_DISTINCT(ARRAY(1.1, 2.2, 2.2, 3.3, 1.1))")
        assert len(result) == 3
        assert set(result) == set(dbe.eval(self._float_array_cast("ARRAY(1.1, 2.2, 3.3)")))
        # assert set(result) == set(dbe.eval("ARRAY(1.1, 2.2, 3.3)"))

        # Single element array
        assert dbe.eval(self._float_array_cast("ARR_DISTINCT(ARRAY(42.5))")) == dbe.eval(
            self._float_array_cast("ARRAY(42.5)")
        )
        # assert dbe.eval("ARR_DISTINCT(ARRAY(42.5))") == dbe.eval("ARRAY(42.5)")

        # All same elements should return single element
        assert dbe.eval(self._float_array_cast("ARR_DISTINCT(ARRAY(5.5, 5.5, 5.5))")) == dbe.eval(
            self._float_array_cast("ARRAY(5.5)")
        )
        # assert dbe.eval("ARR_DISTINCT(ARRAY(5.5, 5.5, 5.5))") == dbe.eval("ARRAY(5.5)")

        # With NULL values
        result = dbe.eval(self._float_array_cast("ARR_DISTINCT(ARRAY(1.1, NULL, 1.1, NULL, 2.2))"))
        # result = dbe.eval("ARR_DISTINCT(ARRAY(1.1, NULL, 1.1, NULL, 2.2))")
        assert len(result) == 3
        assert set(result) == set(dbe.eval(self._float_array_cast("ARRAY(1.1, NULL, 2.2)")))
        # assert set(result) == set(dbe.eval("ARRAY(1.1, NULL, 2.2)"))

    def test_array_distinct_str(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        # Takes array input, returns array with unique values
        result = dbe.eval('ARR_DISTINCT(ARRAY("a", "b", "b", "c", "a"))')
        # Should contain unique values: "a", "b", "c" (order may vary)
        assert len(result) == 3
        assert set(result) == {"a", "b", "c"}

        # Single element array
        assert dbe.eval('ARR_DISTINCT(ARRAY("hello"))') == ["hello"]

        # All same elements should return single element
        assert dbe.eval('ARR_DISTINCT(ARRAY("test", "test", "test"))') == ["test"]

        # With NULL values
        result = dbe.eval('ARR_DISTINCT(ARRAY("a", NULL, "a", NULL, "b"))')
        assert len(result) == 3  # Should have "a", NULL, "b"
        assert set(result) == {"a", "b", None}

        # Empty strings are distinct from NULL
        result = dbe.eval('ARR_DISTINCT(ARRAY("", "", "a", ""))')
        assert len(result) == 2  # Should have "", "a"
        assert set(result) == {"", "a"}

    def test_array_index_of_int(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        # Basic functionality
        assert dbe.eval("ARR_INDEX_OF(ARRAY(10, 20, 30), 20)") == 2
        assert dbe.eval("ARR_INDEX_OF(ARRAY(10, 20, 30), 10)") == 1
        assert dbe.eval("ARR_INDEX_OF(ARRAY(10, 20, 30), 30)") == 3
        # Element not found
        assert dbe.eval("ARR_INDEX_OF(ARRAY(10, 20, 30), 40)") == 0
        # First occurrence
        assert dbe.eval("ARR_INDEX_OF(ARRAY(10, 20, 20, 30), 20)") == 2
        # Single element
        assert dbe.eval("ARR_INDEX_OF(ARRAY(42), 42)") == 1
        assert dbe.eval("ARR_INDEX_OF(ARRAY(42), 99)") == 0
        # NULL handling
        assert dbe.eval("ARR_INDEX_OF(ARRAY(10, NULL, 30), NULL)") == 2
        assert dbe.eval("ARR_INDEX_OF(ARRAY(10, 20, 30), NULL)") == 0
        # With column data
        assert dbe.eval("ARR_INDEX_OF([arr_int_value], 23)", from_=data_table) == 2
        assert dbe.eval("ARR_INDEX_OF([arr_int_value], 456)", from_=data_table) == 3
        assert dbe.eval("ARR_INDEX_OF([arr_int_value], 999)", from_=data_table) == 0

    @pytest.mark.xfail(reason="BI-6163, BI-6165")
    def test_array_index_of_int_null(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        # null array values
        assert dbe.eval("ARR_INDEX_OF([arr_int_null_value], 1)", from_=data_table) is None

    def test_array_index_of_float(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        # Basic functionality
        assert dbe.eval("ARR_INDEX_OF(ARRAY(1.1, 2.2, 3.3), 2.2)") == 2
        assert dbe.eval("ARR_INDEX_OF(ARRAY(1.1, 2.2, 3.3), 1.1)") == 1
        assert dbe.eval("ARR_INDEX_OF(ARRAY(1.1, 2.2, 3.3), 3.3)") == 3
        # Element not found
        assert dbe.eval("ARR_INDEX_OF(ARRAY(1.1, 2.2, 3.3), 4.4)") == 0
        # First occurrence
        assert dbe.eval("ARR_INDEX_OF(ARRAY(1.1, 2.2, 2.2, 3.3), 2.2)") == 2
        # Single element
        assert dbe.eval("ARR_INDEX_OF(ARRAY(4.2), 4.2)") == 1
        assert dbe.eval("ARR_INDEX_OF(ARRAY(4.2), 9.9)") == 0
        # NULL handling
        assert dbe.eval("ARR_INDEX_OF(ARRAY(1.1, NULL, 3.3), NULL)") == 2
        assert dbe.eval("ARR_INDEX_OF(ARRAY(1.1, 2.2, 3.3), NULL)") == 0
        # With column data
        assert dbe.eval("ARR_INDEX_OF([arr_float_value], 45)", from_=data_table) == 2
        assert dbe.eval("ARR_INDEX_OF([arr_float_value], 0.123)", from_=data_table) == 3
        assert dbe.eval("ARR_INDEX_OF([arr_float_value], 9.99)", from_=data_table) == 0

    @pytest.mark.xfail(reason="BI-6163, BI-6165")
    def test_array_index_of_float_null(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        # null array values
        assert dbe.eval("ARR_INDEX_OF([arr_float_null_value], 1.2)", from_=data_table) is None

    def test_array_index_of_str(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        # Basic functionality
        assert dbe.eval('ARR_INDEX_OF(ARRAY("a", "b", "c"), "b")') == 2
        assert dbe.eval('ARR_INDEX_OF(ARRAY("a", "b", "c"), "a")') == 1
        assert dbe.eval('ARR_INDEX_OF(ARRAY("a", "b", "c"), "c")') == 3
        # Element not found
        assert dbe.eval('ARR_INDEX_OF(ARRAY("a", "b", "c"), "d")') == 0
        # First occurrence
        assert dbe.eval('ARR_INDEX_OF(ARRAY("a", "b", "b", "c"), "b")') == 2
        # Single element
        assert dbe.eval('ARR_INDEX_OF(ARRAY("hello"), "hello")') == 1
        assert dbe.eval('ARR_INDEX_OF(ARRAY("hello"), "world")') == 0
        # Empty string
        assert dbe.eval('ARR_INDEX_OF(ARRAY("", "b", "c"), "")') == 1
        # Case sensitivity
        assert dbe.eval('ARR_INDEX_OF(ARRAY("a", "B", "c"), "b")') == 0
        assert dbe.eval('ARR_INDEX_OF(ARRAY("a", "B", "c"), "B")') == 2
        # NULL handling
        assert dbe.eval('ARR_INDEX_OF(ARRAY("a", NULL, "c"), NULL)') == 2
        assert dbe.eval('ARR_INDEX_OF(ARRAY("a", "b", "c"), NULL)') == 0
        # With column data
        assert dbe.eval('ARR_INDEX_OF([arr_str_value], "cde")', from_=data_table) == 3
        assert dbe.eval('ARR_INDEX_OF([arr_str_value], "xyz")', from_=data_table) == 0

    @pytest.mark.xfail(reason="BI-6163, BI-6165")
    def test_array_index_of_str_null(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        # null array values
        assert dbe.eval('ARR_INDEX_OF([arr_str_null_value], "cat")', from_=data_table) is None
