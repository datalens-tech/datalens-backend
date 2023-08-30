from __future__ import annotations

import abc
from typing import Any, ClassVar, Optional, Union

import pytest
import sqlalchemy as sa

import bi_formula.core.exc as exc
from bi_formula.testing.util import to_str
from bi_formula.connectors.base.testing.base import FormulaConnectorTestBase
from bi_formula.testing.evaluator import DbEvaluator


SAMPLE_DATA_ARRAYS_LENGTH = 4


class DefaultArrayFunctionFormulaConnectorTestSuite(FormulaConnectorTestBase):
    make_decimal_cast: ClassVar[Optional[str]] = None
    make_float_cast: ClassVar[Optional[str]] = None

    def test_create_array(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        sample_array: tuple[Union[str, int, float, None], ...]
        expected: tuple[Union[str, int, float, None], ...]
        item: Any
        exp: Any
        for sample_array in (
            (0, 23, 456, None),
            (0, 43, 0.123, None),
        ):
            array_string = ','.join('NULL' if item is None else str(item) for item in sample_array)
            for i, item in enumerate(sample_array):
                assert dbe.eval(f'GET_ITEM(ARRAY({array_string}), {i + 1})', from_=data_table) == item

        # testing string separately because they must be quoted
        sample_array = ('', 'NULL', 'cde', None)
        array_string = ','.join('NULL' if item is None else f"'{item}'" for item in sample_array)
        for i, item in enumerate(sample_array):
            assert dbe.eval(f'GET_ITEM(ARRAY({array_string}), {i + 1})', from_=data_table) == item

        # it is not allowed to create array with undefined element type
        with pytest.raises(exc.TranslationError):
            dbe.eval('ARRAY(NULL, NULL, NULL)')

        expected = (0, 1, None)
        for i, exp in enumerate(expected):
            assert dbe.eval(f'GET_ITEM(ARRAY([int_value], 1, NULL), {i + 1})', from_=data_table) == exp

        expected = ('q', '1', None)
        for i, exp in enumerate(expected):
            assert dbe.eval(f'GET_ITEM(ARRAY([str_value], STR(1), NULL), {i + 1})', from_=data_table) == exp

    def test_unnest_array(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        for array_field, inner_type in (
                ('arr_int_value', int),
                ('arr_float_value', float),
                ('arr_str_value', str),
        ):
            count_before_unnest = len(dbe.eval(f'[{array_field}]', from_=data_table, many=True))
            res = dbe.eval(f'UNNEST([{array_field}])', from_=data_table, many=True)
            assert len(res) == count_before_unnest * SAMPLE_DATA_ARRAYS_LENGTH
            assert all(isinstance(value, inner_type) or value is None for value in res)

    def test_array_len(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval('LEN([arr_int_value])', from_=data_table) == SAMPLE_DATA_ARRAYS_LENGTH
        assert dbe.eval('LEN([arr_float_value])', from_=data_table) == SAMPLE_DATA_ARRAYS_LENGTH
        assert dbe.eval('LEN([arr_str_value])', from_=data_table) == SAMPLE_DATA_ARRAYS_LENGTH

    def test_array_get_item(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval('GET_ITEM([arr_int_value], 3)', from_=data_table) == 456
        assert dbe.eval('GET_ITEM([arr_float_value], 3)', from_=data_table) == 0.123
        assert dbe.eval('GET_ITEM([arr_str_value], 3)', from_=data_table) == 'cde'

    def test_array_str(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert to_str(dbe.eval('ARR_STR([arr_int_value])', from_=data_table)) == '0,23,456'
        assert to_str(dbe.eval('ARR_STR([arr_float_value])', from_=data_table)) == '0,45,0.123'
        assert to_str(dbe.eval('ARR_STR([arr_str_value])', from_=data_table)) == ',,cde'

        assert to_str(dbe.eval("ARR_STR([arr_int_value], ';')", from_=data_table)) == '0;23;456'
        assert to_str(dbe.eval("ARR_STR([arr_float_value], ';')", from_=data_table)) == '0;45;0.123'
        assert to_str(dbe.eval("ARR_STR([arr_str_value], ';')", from_=data_table)) == ';;cde'

        assert to_str(dbe.eval("ARR_STR([arr_int_value], ';', '*')", from_=data_table)) == '0;23;456;*'
        assert to_str(dbe.eval("ARR_STR([arr_float_value], ';', '*')", from_=data_table)) == '0;45;0.123;*'
        assert to_str(dbe.eval("ARR_STR([arr_str_value], ';', '*')", from_=data_table)) == ';;cde;*'

    def test_array_count_item(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval('COUNT_ITEM([arr_int_value], 23)', from_=data_table) == 1
        assert dbe.eval('COUNT_ITEM([arr_float_value], 45)', from_=data_table) == 1
        assert dbe.eval('COUNT_ITEM([arr_str_value], "cde")', from_=data_table) == 1
        assert dbe.eval('COUNT_ITEM([arr_str_value], NULL)', from_=data_table) == 1

        assert dbe.eval('COUNT_ITEM(ARRAY(1, 2, 3), NULL)', from_=data_table) == 0
        assert dbe.eval('COUNT_ITEM(ARRAY(1, NULL, NULL), NULL)', from_=data_table) == 2

    def test_array_contains(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval('CONTAINS(ARRAY(1, 2, 3), 1)', from_=data_table)
        assert dbe.eval('CONTAINS(ARRAY(1.1, 2.2, 3.3), 3.3)', from_=data_table)
        assert dbe.eval('CONTAINS(ARRAY("a", "b", "c"), "a")', from_=data_table)
        assert not dbe.eval('CONTAINS(ARRAY("a", "b", "c"), "d")', from_=data_table)
        assert dbe.eval('CONTAINS(ARRAY("a", NULL, "c"), NULL)', from_=data_table)
        assert not dbe.eval('CONTAINS(ARRAY(1.1, 2.2, 3.3), NULL)', from_=data_table)

        assert dbe.eval('CONTAINS([arr_int_value], 23)', from_=data_table)
        assert not dbe.eval('CONTAINS([arr_int_value], 24)', from_=data_table)
        assert dbe.eval('CONTAINS([arr_str_value], "cde")', from_=data_table)
        assert dbe.eval('CONTAINS([arr_str_value], NULL)', from_=data_table)

        assert dbe.eval('CONTAINS([arr_str_value], GET_ITEM([arr_str_value], 1))', from_=data_table)
        assert dbe.eval('CONTAINS([arr_str_value], GET_ITEM([arr_str_value], 2))', from_=data_table)
        assert dbe.eval('CONTAINS([arr_str_value], GET_ITEM([arr_str_value], 3))', from_=data_table)
        assert dbe.eval('CONTAINS([arr_str_value], GET_ITEM([arr_str_value], 4))', from_=data_table)

    def test_array_contains_all(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval('CONTAINS_ALL(ARRAY(1, 2, 3), ARRAY(1, 2))', from_=data_table)
        assert dbe.eval('CONTAINS_ALL(ARRAY(1.1, 2.2, 3.3), ARRAY(1.1, 2.2))', from_=data_table)
        assert dbe.eval('CONTAINS_ALL(ARRAY("a", "b", "c"), ARRAY("a", "b"))', from_=data_table)

        assert dbe.eval('CONTAINS_ALL(ARRAY(1, 2, 3), ARRAY(2, 1))', from_=data_table)
        assert dbe.eval('CONTAINS_ALL(ARRAY(1.1, 2.2, 3.3), ARRAY(3.3, 3.3, 3.3, 3.3, 1.1))', from_=data_table)
        assert dbe.eval('CONTAINS_ALL(ARRAY("a", "b", "c"), ARRAY("c"))', from_=data_table)

        assert not dbe.eval('CONTAINS_ALL(ARRAY("a", "b", "c"), ARRAY("a", "d"))', from_=data_table)
        assert dbe.eval('CONTAINS_ALL(ARRAY("a", NULL, "c"), ARRAY("a", NULL))', from_=data_table)
        assert dbe.eval('CONTAINS_ALL(ARRAY("a", NULL, "c"), ARRAY("a", "c"))', from_=data_table)
        assert not dbe.eval('CONTAINS_ALL(ARRAY(1.1, 2.2, 3.3), ARRAY(1.1, NULL))', from_=data_table)

        assert dbe.eval('CONTAINS_ALL([arr_int_value], ARRAY(23, 456))', from_=data_table)
        assert not dbe.eval('CONTAINS_ALL([arr_int_value], ARRAY(24, 456))', from_=data_table)

        assert dbe.eval('CONTAINS_ALL([arr_int_value], SLICE([arr_int_value], 2, 2))', from_=data_table)
        assert dbe.eval('CONTAINS_ALL(ARRAY(0, 23, 456, NULL, 123), [arr_int_value])', from_=data_table)

    def test_array_contains_all_string_array(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval('CONTAINS_ALL([arr_str_value], ARRAY("cde"))', from_=data_table)
        assert dbe.eval('CONTAINS_ALL([arr_str_value], ARRAY("cde", NULL))', from_=data_table)
        assert not dbe.eval('CONTAINS_ALL(ARRAY("cde"), [arr_str_value])', from_=data_table)

    def test_array_contains_any(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval('CONTAINS_ANY(ARRAY(1, 2, 3), ARRAY(1, 5))', from_=data_table)
        assert dbe.eval('CONTAINS_ANY(ARRAY(1.1, 2.2, 3.3), ARRAY(1.1, 5.5))', from_=data_table)
        assert dbe.eval('CONTAINS_ANY(ARRAY("a", "b", "c"), ARRAY("a", "e"))', from_=data_table)

        assert not dbe.eval('CONTAINS_ANY(ARRAY(1, 2, 3), ARRAY(6, 5))', from_=data_table)
        assert not dbe.eval('CONTAINS_ANY(ARRAY(1.1, 2.2, 3.3), ARRAY(6.6, 5.5))', from_=data_table)
        assert not dbe.eval('CONTAINS_ANY(ARRAY("a", "b", "c"), ARRAY("f", "e"))', from_=data_table)

        assert dbe.eval('CONTAINS_ANY(ARRAY("a", NULL, "c"), ARRAY("e", NULL))', from_=data_table)
        assert dbe.eval('CONTAINS_ANY(ARRAY("a", NULL, "c"), ARRAY("e", "c"))', from_=data_table)
        assert not dbe.eval('CONTAINS_ANY(ARRAY(1.1, 2.2, 3.3), ARRAY(5.5, NULL))', from_=data_table)

        assert dbe.eval('CONTAINS_ANY([arr_int_value], ARRAY(24, 456))', from_=data_table)
        assert not dbe.eval('CONTAINS_ANY([arr_int_value], ARRAY(24, 457))', from_=data_table)

        assert dbe.eval('CONTAINS_ANY([arr_int_value], SLICE([arr_int_value], 2, 1))', from_=data_table)
        assert dbe.eval('CONTAINS_ANY(ARRAY(0, 23, 456, NULL, 123), [arr_int_value])', from_=data_table)

    def test_array_contains_any_string_array(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval('CONTAINS_ANY([arr_str_value], ARRAY("cde"))', from_=data_table)
        assert dbe.eval('CONTAINS_ANY([arr_str_value], ARRAY("123", NULL))', from_=data_table)
        assert dbe.eval('CONTAINS_ANY(ARRAY("cde"), [arr_str_value])', from_=data_table)

    def test_array_slice(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval('SLICE([arr_int_value], 2, 2)', from_=data_table) == dbe.eval('ARRAY(23, 456)', from_=data_table)
        assert dbe.eval('SLICE(ARRAY(1, 2, 3, 4), 2, 2)', from_=data_table) == dbe.eval('ARRAY(2, 3)', from_=data_table)
        assert dbe.eval('SLICE(ARRAY(1, 2, 3, 4), 2, 1)', from_=data_table) == dbe.eval('ARRAY(2)', from_=data_table)
        assert str(dbe.eval('SLICE(ARRAY(1, 2, 3, 4), 2, 0)', from_=data_table)) == '[]'

    def test_replace_array(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval('REPLACE([arr_int_value], -2, 1000)', from_=data_table) == dbe.eval('ARRAY(0, 23, 456, NULL)')
        assert dbe.eval('REPLACE([arr_int_value], 456, 1000)', from_=data_table) == dbe.eval('ARRAY(0, 23, 1000, NULL)')
        assert dbe.eval('REPLACE(ARRAY(6, 6, 6), 6, 7)') == dbe.eval('ARRAY(7, 7, 7)')
        assert dbe.eval('REPLACE(ARRAY(NULL, NULL, 1), NULL, 1)') == dbe.eval('ARRAY(1, 1, 1)')
        assert dbe.eval('REPLACE(ARRAY("NULL", NULL, "NULL"), "NULL", "LUL")') == dbe.eval('ARRAY("LUL", NULL, "LUL")')
        assert dbe.eval('REPLACE(ARRAY(45, 1, 45), GET_ITEM(ARRAY(24, 45), 2), 22 + 2)') == dbe.eval('ARRAY(24, 1, 24)')

    def test_array_cast(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval(
            'CAST_ARR_INT([arr_float_value])', from_=data_table
        ) == dbe.eval('ARRAY(0, 45, 0, NULL)', from_=data_table)
        assert dbe.eval(
            'CAST_ARR_INT([arr_int_value])', from_=data_table
        ) == dbe.eval('[arr_int_value]', from_=data_table)
        assert dbe.eval(
            'CAST_ARR_INT(ARRAY("1", "3", "2", NULL))', from_=data_table
        ) == dbe.eval('ARRAY(1, 3, 2, NULL)', from_=data_table)

        assert dbe.eval(
            'CAST_ARR_FLOAT([arr_float_value])', from_=data_table
        ) == dbe.eval('[arr_float_value]', from_=data_table)
        assert dbe.eval(
            'CAST_ARR_FLOAT([arr_int_value])', from_=data_table
        ) == dbe.eval('ARRAY(0, 23.0, 456, NULL)', from_=data_table)
        assert dbe.eval(
            'CAST_ARR_FLOAT(ARRAY("1", "3.3", "2", NULL))', from_=data_table
        ) == dbe.eval('ARRAY(1, 3.3, 2, NULL)', from_=data_table)

        assert dbe.eval('CAST_ARR_STR([arr_float_value])', from_=data_table) in (
            dbe.eval('ARRAY("0.0", "45.0", "0.123", NULL)', from_=data_table),
            dbe.eval('ARRAY("0", "45", "0.123", NULL)', from_=data_table)
        )
        assert dbe.eval('CAST_ARR_STR([arr_int_value])', from_=data_table) == dbe.eval('ARRAY("0", "23", "456", NULL)', from_=data_table)
        assert dbe.eval('CAST_ARR_STR([arr_str_value])', from_=data_table) == dbe.eval('[arr_str_value]', from_=data_table)

    def test_array_get_item_nested(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        src = "ARRAY('qwe', 'rty', 'uio')"

        assert dbe.eval(f'GET_ITEM({src}, 1)') == "qwe"
        assert dbe.eval('GET_ITEM([arr_int_value], 3)', from_=data_table) == 456

        assert dbe.eval(f'GET_ITEM(SLICE({src}, 2, 1), 1)') == 'rty'
        assert dbe.eval('GET_ITEM(SLICE([arr_int_value], 2, 2), 2)', from_=data_table) == 456

        assert dbe.eval(f'SLICE(SLICE({src}, 2, 1), 1, 1)') == dbe.eval('ARRAY("rty")', from_=data_table)
        assert dbe.eval(
            'SLICE(SLICE([arr_int_value], 2, 1), 1, 1)', from_=data_table
        ) == dbe.eval(
            'ARRAY(23)', from_=data_table
        )

        assert dbe.eval('STARTSWITH(SLICE(ARRAY(1, 2, 3, 4, 5), 1, 4), SLICE(ARRAY(1, 2, 3), 1, 2))')
        assert not dbe.eval('STARTSWITH(SLICE([arr_int_value], 1, 4), ARRAY(1, 2, 3))', from_=data_table)
        assert dbe.eval('STARTSWITH(SLICE([arr_int_value], 1, 3), ARRAY(0, 23, 456))', from_=data_table)

    def test_startswith_number_array(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval('STARTSWITH(ARRAY(1, 2, 3, 4, 5), ARRAY(1, 2, 3))', from_=data_table)
        assert dbe.eval('STARTSWITH(ARRAY(1, 2, 3, 4, 5), ARRAY(1, 2, 3, 4, 5))', from_=data_table)
        assert dbe.eval('STARTSWITH(ARRAY(1, NULL, 3, NULL, 5), ARRAY(1, NULL, 3, NULL))', from_=data_table)
        assert not dbe.eval('STARTSWITH(ARRAY(1, 2, 3, 4, 5), ARRAY(1, 2, 3, 4, 5, 6, 7))', from_=data_table)
        assert not dbe.eval('STARTSWITH(ARRAY(1, 2, 3, 4, 5), ARRAY(5, 4, 3, 2, 1))', from_=data_table)
        assert not dbe.eval('STARTSWITH(ARRAY(1, 2, 3, 4, 5), ARRAY(3, 4))', from_=data_table)
        assert not dbe.eval('STARTSWITH(ARRAY(1, 2, NULL, 4, 5), ARRAY(2, NULL))', from_=data_table)
        assert dbe.eval('STARTSWITH(ARRAY(1.1, NULL, 3.3, 4.4, 5.5), ARRAY(1.1, NULL, 3.3))', from_=data_table)
        assert dbe.eval('STARTSWITH(ARRAY("", "NULL", NULL, "12"), ARRAY("", "NULL"))', from_=data_table)

        assert dbe.eval('STARTSWITH([arr_int_value], [arr_int_value])', from_=data_table)
        assert dbe.eval('STARTSWITH([arr_float_value], [arr_float_value])', from_=data_table)

    @abc.abstractmethod
    def test_startswith_string_array(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        pass  # Defined in a dialect-specific way

    def test_array_remove(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        # with ARRAY func | remove literal not NULL
        assert dbe.eval('ARR_REMOVE(ARRAY(1, 2, NULL), 1)', from_=data_table) == dbe.eval('ARRAY(2, NULL)')
        assert dbe.eval('ARR_REMOVE(ARRAY("a", "b", NULL), "a")', from_=data_table) == dbe.eval('ARRAY("b", NULL)')
        if self.make_decimal_cast:
            assert dbe.eval(
                'ARR_REMOVE(ARRAY(1.1, 2.2, NULL), 1.1)', from_=data_table
            ) == dbe.eval(f'ARRAY(DB_CAST(2.2, "{self.make_decimal_cast}", 2, 1), NULL)')
        else:
            assert dbe.eval(
                'ARR_REMOVE(ARRAY(1.1, 2.2, NULL), 1.1)', from_=data_table
            ) == dbe.eval('ARRAY(2.2, NULL)')

        # with ARRAY func | remove literal NULL
        assert dbe.eval('ARR_REMOVE(ARRAY(1, 2, NULL), NULL)', from_=data_table) == dbe.eval('ARRAY(1, 2)')
        assert dbe.eval('ARR_REMOVE(ARRAY("a", "b", NULL), NULL)', from_=data_table) == dbe.eval('ARRAY("a", "b")')
        if self.make_decimal_cast:
            assert dbe.eval(
                'ARR_REMOVE(ARRAY(1.1, 2.2, NULL), NULL)', from_=data_table
            ) == dbe.eval(
                f'ARRAY(DB_CAST(1.1, "{self.make_decimal_cast}", 2, 1), DB_CAST(2.2, "{self.make_decimal_cast}", 2, 1))'
            )
        else:
            assert dbe.eval(
                'ARR_REMOVE(ARRAY(1.1, 2.2, NULL), NULL)', from_=data_table
            ) == dbe.eval('ARRAY(1.1, 2.2)')

        # with array in DB | remove literal not NULL
        assert dbe.eval('ARR_REMOVE([arr_int_value], 0)', from_=data_table) == dbe.eval('ARRAY(23, 456, NULL)')
        assert dbe.eval('ARR_REMOVE([arr_str_value], "")', from_=data_table) == dbe.eval('ARRAY("cde", NULL)')
        if self.make_float_cast:
            assert dbe.eval(
                f'ARR_REMOVE([arr_float_value], DB_CAST(0.0, "{self.make_float_cast}"))', from_=data_table
            ) == dbe.eval('ARRAY(45, 0.123, NULL)')
        else:
            assert dbe.eval(
                'ARR_REMOVE([arr_float_value], 0.0)', from_=data_table
            ) == dbe.eval('ARRAY(45, 0.123, NULL)')

        # with array in DB | remove literal NULL
        assert dbe.eval('ARR_REMOVE([arr_int_value], NULL)', from_=data_table) == dbe.eval('ARRAY(0, 23, 456)')
        assert dbe.eval('ARR_REMOVE([arr_str_value], NULL)', from_=data_table) == dbe.eval('ARRAY("", "", "cde")')
        assert dbe.eval('ARR_REMOVE([arr_float_value], NULL)', from_=data_table) == dbe.eval('ARRAY(0, 45, 0.123)')

        # with array in DB | remove non const
        assert dbe.eval(
            'ARR_REMOVE([arr_int_value], GET_ITEM([arr_int_value], 1))', from_=data_table
        ) == dbe.eval('ARRAY(23, 456, NULL)')
        assert dbe.eval(
            'ARR_REMOVE([arr_str_value], GET_ITEM([arr_str_value], 1))', from_=data_table
        ) == dbe.eval('ARRAY("cde", NULL)')
        assert dbe.eval(
            'ARR_REMOVE([arr_float_value], GET_ITEM([arr_float_value], 1))', from_=data_table
        ) == dbe.eval('ARRAY(45, 0.123, NULL)')
