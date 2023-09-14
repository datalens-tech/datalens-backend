from __future__ import annotations

import datetime
from typing import (
    Any,
    ClassVar,
    Optional,
)

import pytest
import sqlalchemy as sa

from bi_formula_testing.evaluator import DbEvaluator
from bi_formula_testing.testcases.base import FormulaConnectorTestBase
from bi_formula_testing.util import (
    dt_strip,
    to_str,
)


class DefaultOperatorFormulaConnectorTestSuite(FormulaConnectorTestBase):
    subtraction_round_dt: ClassVar[bool] = True
    supports_string_int_multiplication: ClassVar[bool] = True
    supports_null_in: ClassVar[bool] = True
    make_float_array_cast: Optional[str] = None

    def test_negation(self, dbe: DbEvaluator, forced_literal_use: Any) -> None:
        assert dbe.eval("-__LIT__(2)") == -2

    def test_addition_number_number(self, dbe: DbEvaluator, forced_literal_use: Any) -> None:
        assert dbe.eval("__LIT__(2) + 3") == 5

    def test_addition_string_string(self, dbe: DbEvaluator, forced_literal_use: Any) -> None:
        assert to_str(dbe.eval('__LIT__("Lorem") + " ipsum"')) == "Lorem ipsum"

    def test_addition_date_number(self, dbe: DbEvaluator) -> None:
        assert dbe.eval("#2019-01-06# + 2") == datetime.date(2019, 1, 8)
        assert dbe.eval("2 + #2019-01-06#") == datetime.date(2019, 1, 8)
        assert dbe.eval("#2019-01-06# + 2.2") == datetime.date(2019, 1, 8)
        assert dbe.eval("2.2 + #2019-01-06#") == datetime.date(2019, 1, 8)

    def test_addition_datetime_number(self, dbe: DbEvaluator) -> None:
        assert dt_strip(dbe.eval("#2019-01-06 03# + 2")) == datetime.datetime(2019, 1, 8, 3)
        assert dt_strip(dbe.eval("2 + #2019-01-06 03#")) == datetime.datetime(2019, 1, 8, 3)
        assert dt_strip(dbe.eval("#2019-01-06 03# + 2.5")) == datetime.datetime(2019, 1, 8, 15)
        assert dt_strip(dbe.eval("2.5 + #2019-01-06 03#")) == datetime.datetime(2019, 1, 8, 15)
        assert dt_strip(dbe.eval("2.0005 + #2019-01-06 03#")) == datetime.datetime(2019, 1, 8, 3, 0, 43)
        assert dt_strip(dbe.eval("#2019-01-06 03# + 2.0005")) == datetime.datetime(2019, 1, 8, 3, 0, 43)

    def test_addition_genericdatetime_number(self, dbe: DbEvaluator) -> None:
        assert dt_strip(dbe.eval("##2019-01-06 03## + 2")) == datetime.datetime(2019, 1, 8, 3)
        assert dt_strip(dbe.eval("2 + ##2019-01-06 03##")) == datetime.datetime(2019, 1, 8, 3)
        assert dt_strip(dbe.eval("##2019-01-06 03## + 2.5")) == datetime.datetime(2019, 1, 8, 15)
        assert dt_strip(dbe.eval("2.5 + ##2019-01-06 03##")) == datetime.datetime(2019, 1, 8, 15)
        assert dt_strip(dbe.eval("2.0005 + ##2019-01-06 03##")) == datetime.datetime(2019, 1, 8, 3, 0, 43)
        assert dt_strip(dbe.eval("##2019-01-06 03## + 2.0005")) == datetime.datetime(2019, 1, 8, 3, 0, 43)

    def test_subtraction_number_number(self, dbe: DbEvaluator, forced_literal_use: Any) -> None:
        assert dbe.eval("__LIT__(2) - 3") == -1

    def test_subtraction_date_number(self, dbe: DbEvaluator) -> None:
        assert dbe.eval("#2019-01-06# - 2") == datetime.date(2019, 1, 4)
        assert dbe.eval("#2019-01-06# - 2.2") == datetime.date(2019, 1, 3)

    def test_subtraction_datetime_number(self, dbe: DbEvaluator) -> None:
        assert dt_strip(dbe.eval("#2019-01-06 03# - 2")) == datetime.datetime(2019, 1, 4, 3)
        assert dt_strip(dbe.eval("#2019-01-06 03# - 2.5")) == datetime.datetime(2019, 1, 3, 15)
        if not self.subtraction_round_dt:
            assert dt_strip(dbe.eval("#2019-01-06 03# - 2.0005")) == datetime.datetime(2019, 1, 4, 2, 59, 16)
        else:
            assert dt_strip(dbe.eval("#2019-01-06 03# - 2.0005")) == datetime.datetime(2019, 1, 4, 2, 59, 17)

    def test_subtraction_genericdatetime_number(self, dbe: DbEvaluator) -> None:
        assert dt_strip(dbe.eval("##2019-01-06 03## - 2")) == datetime.datetime(2019, 1, 4, 3)
        assert dt_strip(dbe.eval("##2019-01-06 03## - 2.5")) == datetime.datetime(2019, 1, 3, 15)
        if not self.subtraction_round_dt:
            assert dt_strip(dbe.eval("##2019-01-06 03## - 2.0005")) == datetime.datetime(2019, 1, 4, 2, 59, 16)
        else:
            assert dt_strip(dbe.eval("##2019-01-06 03## - 2.0005")) == datetime.datetime(2019, 1, 4, 2, 59, 17)

    def test_subtraction_date_time_date_time(self, dbe: DbEvaluator) -> None:
        assert dbe.eval("#2019-01-06# - #2019-01-02#") == 4
        assert dbe.eval("#2019-01-06 15# - #2019-01-02 03#") == pytest.approx(4.5)

    def test_subtraction_genericdatetime_genericdatetime(self, dbe: DbEvaluator) -> None:
        assert dbe.eval("##2019-01-06## - ##2019-01-02##") == 4
        assert dbe.eval("##2019-01-06 15## - ##2019-01-02 03##") == pytest.approx(4.5)

    def test_multiplication(self, dbe: DbEvaluator, forced_literal_use: Any) -> None:
        assert dbe.eval("__LIT__(2) * 3") == 6
        assert dbe.eval("__LIT__(2.1) * 3") == pytest.approx(6.3)
        assert to_str(dbe.eval('"Lorem" * 3')) == "LoremLoremLorem"
        assert to_str(dbe.eval('3 * "Lorem"')) == "LoremLoremLorem"
        if self.supports_string_int_multiplication:
            assert to_str(dbe.eval('__LIT__("Lorem") * 3')) == "LoremLoremLorem"
            assert to_str(dbe.eval('__LIT__(3) * "Lorem"')) == "LoremLoremLorem"

    def test_division(self, dbe: DbEvaluator, forced_literal_use: Any) -> None:
        assert dbe.eval("__LIT__(4) / 2") == 2
        assert dbe.eval("__LIT__(5) / 2") == pytest.approx(2.5)
        assert dbe.eval("__LIT__(5.0) / 2") == pytest.approx(2.5)
        assert dbe.eval("__LIT__(5) / 2.0") == pytest.approx(2.5)

    def test_modulo(self, dbe: DbEvaluator, forced_literal_use: Any) -> None:
        assert dbe.eval("__LIT__(2) % 3") == 2
        assert dbe.eval("__LIT__(2.1) % 3") == pytest.approx(2.1)
        assert dbe.eval("__LIT__(3) % 2.1") == pytest.approx(0.9)

    def test_power(self, dbe: DbEvaluator, forced_literal_use: Any) -> None:
        assert dbe.eval("__LIT__(2) ^ 3") == 8
        assert dbe.eval("__LIT__(2.1) ^ -0.3") == pytest.approx(2.1**-0.3)

    def test_grouping_with_operators(self, dbe: DbEvaluator) -> None:
        """Make sure that operand grouping is performed correctly with field substitution"""
        assert (
            dbe.eval(
                "[f1] / [f2]",
                other_fields={
                    "f1": "ROUND(1000)",
                    "f2": "(ROUND(100) / 10) * 10",
                },
            )
            == 10
        )

    def test_is_null(self, dbe: DbEvaluator, forced_literal_use: Any) -> None:
        assert not dbe.eval('__LIT__("qwerty") IS NULL')
        assert dbe.eval("__LIT__(NULL) IS NULL")
        assert dbe.eval('__LIT__("qwerty") IS NOT NULL')
        assert not dbe.eval("__LIT__(NULL) IS NOT NULL")

    def test_is_bool(self, dbe: DbEvaluator, forced_literal_use: Any) -> None:
        assert dbe.eval('__LIT__("qwerty") IS TRUE')
        assert dbe.eval("__LIT__(123) IS TRUE")
        assert dbe.eval("__LIT__(TRUE) IS TRUE")
        assert not dbe.eval("__LIT__(FALSE) IS TRUE")
        assert dbe.eval("__LIT__(FALSE) IS NOT TRUE")
        assert dbe.eval("#2019-03-05# IS TRUE")
        assert dbe.eval("#2019-03-05 01:02:03# IS TRUE")
        assert dbe.eval("##2019-03-05 01:02:03## IS TRUE")
        assert dbe.eval('__LIT__("") IS FALSE')
        assert dbe.eval("__LIT__(0) IS FALSE")
        assert dbe.eval("__LIT__(FALSE) IS FALSE")
        assert not dbe.eval("__LIT__(TRUE) IS FALSE")
        assert dbe.eval("__LIT__(TRUE) IS NOT FALSE")
        assert not dbe.eval("#2019-03-05# IS FALSE")
        assert not dbe.eval("#2019-03-05 01:02:03# IS FALSE")
        assert not dbe.eval("##2019-03-05 01:02:03## IS FALSE")

    def test_like(self, dbe: DbEvaluator, forced_literal_use: Any) -> None:
        assert dbe.eval('"raspberry pi" LIKE "%spb%"')
        assert not dbe.eval('"raspberry pi" NOT LIKE "%spb%"')

    def test_not(self, dbe: DbEvaluator, forced_literal_use: Any) -> None:
        assert dbe.eval("NOT __LIT__(FALSE)")
        assert not dbe.eval("NOT __LIT__(TRUE)")
        assert dbe.eval('NOT __LIT__("")')
        assert not dbe.eval('NOT __LIT__("text")')
        assert dbe.eval("NOT __LIT__(0)")
        assert not dbe.eval("NOT __LIT__(1)")
        assert not dbe.eval("NOT __LIT__(#2019-01-01#)")
        assert not dbe.eval("NOT __LIT__(#2019-01-01 03:00:00#)")
        assert not dbe.eval("NOT ##2019-01-01 03:00:00##")

    def test_in(self, dbe: DbEvaluator, forced_literal_use: Any) -> None:
        assert dbe.eval("3 IN (23, 5, 3, 67)")
        assert not dbe.eval("3 NOT IN (23, 5, 3, 67)")

        assert not dbe.eval("3 in (1, 2, NULL)")
        assert dbe.eval("3 not in (1, 2, NULL)")

        if self.supports_null_in:
            assert dbe.eval("NULL in (1, 2, NULL)")
            assert not dbe.eval("NULL not in (1, 2, NULL)")

        assert not dbe.eval("3 in (NULL)")
        assert dbe.eval("3 not in (NULL)")

        assert dbe.eval("NULL in (NULL)")
        assert not dbe.eval("NULL not in (NULL)")

    def test_in_date(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval("[date_value] in (#2014-10-05#)", order_by=["[date_value]"], from_=data_table)

    def test_between(self, dbe: DbEvaluator) -> None:
        assert dbe.eval("3 BETWEEN 1 AND 100")
        assert not dbe.eval("3 NOT BETWEEN 1 AND 100")

    def test_comparison_operators(self, dbe: DbEvaluator) -> None:
        assert dbe.eval("1 < 3 <= 4 = 4 != 10 > 7 >= 6")
        assert dbe.eval('"qwerty" = "qwerty"')
        assert dbe.eval('"qwerty" != "abc"')
        assert dbe.eval('"qwerty" > "abc"')
        assert dbe.eval('"qwerty" >= "abc"')
        assert dbe.eval('"abc" < "qwerty"')
        assert dbe.eval('"abc" <= "qwerty"')
        if self.supports_arrays:
            assert dbe.eval("ARRAY(1, 2, NULL, 4) = ARRAY(1, 2, NULL, 4)")

    @pytest.mark.parametrize("lit", (pytest.param("##", id="generic"), pytest.param("#", id="regular")))
    def test_comparison_operators_datetimes(self, dbe: DbEvaluator, lit: str) -> None:
        def _dt_lit(s: str) -> str:
            return f"{lit}{s}{lit}"

        def _assert_eq(first: str, second: str) -> None:
            assert dbe.eval(f"{_dt_lit(first)} = {_dt_lit(second)}")
            assert dbe.eval(f"{_dt_lit(first)} >= {_dt_lit(second)}")
            assert dbe.eval(f"{_dt_lit(first)} <= {_dt_lit(second)}")

        def _assert_neq(first: str, second: str) -> None:
            assert dbe.eval(f"{_dt_lit(first)} != {_dt_lit(second)}")
            assert dbe.eval(f"{_dt_lit(first)} < {_dt_lit(second)}")
            assert dbe.eval(f"{_dt_lit(first)} <= {_dt_lit(second)}")
            assert dbe.eval(f"{_dt_lit(second)} > {_dt_lit(first)}")
            assert dbe.eval(f"{_dt_lit(second)} >= {_dt_lit(first)}")

        # dates
        _assert_eq("2019-08-08", "2019-08-08")
        _assert_neq("2019-08-08", "2019-08-09")

        # datetime
        _assert_eq("2019-08-08 05:00:00", "2019-08-08 05:00:00")
        _assert_neq("2019-08-08 05:00:00", "2019-08-08 05:00:30")

        # date/datetime
        _assert_eq("2019-08-08", "2019-08-08 00:00:00")
        _assert_eq("2019-08-08 00:00:00", "2019-08-08")
        _assert_neq("2019-08-08", "2019-08-08 05:00:30")
        _assert_neq("2019-08-08 05:00:30", "2019-08-09")

    def test_addition_array_array(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        if not self.supports_arrays:
            pytest.skip()

        assert dbe.eval("[arr_int_value] + [arr_int_value]", from_=data_table) == dbe.eval(
            "ARRAY(0, 23, 456, NULL, 0, 23, 456, NULL)"
        )
        assert dbe.eval("ARRAY(1, 2, 3) + ARRAY(6, NULL, 0)", from_=data_table) == dbe.eval(
            "ARRAY(1, 2, 3, 6, NULL, 0)"
        )

        assert dbe.eval("[arr_str_value] + [arr_str_value]", from_=data_table) == dbe.eval(
            'ARRAY("", "", "cde", NULL, "", "", "cde", NULL)'
        )
        assert dbe.eval('ARRAY("a", "", "3") + ARRAY("c", NULL, "e")', from_=data_table) == dbe.eval(
            'ARRAY("a", "", "3", "c", NULL, "e")'
        )

        if self.make_float_array_cast is not None:
            assert dbe.eval("[arr_float_value] + [arr_float_value]", from_=data_table) == dbe.eval(
                f'DB_CAST(ARRAY(0, 45, 0.123, NULL, 0, 45, 0.123, NULL), "{self.make_float_array_cast}")'
            )
            assert dbe.eval(
                f'DB_CAST(ARRAY(1.1, 2.2, 3.3) + ARRAY(6.6, NULL, 0), "{self.make_float_array_cast}")', from_=data_table
            ) == dbe.eval("ARRAY(1.1, 2.2, 3.3, 6.6, NULL, 0)")
        else:
            assert dbe.eval("[arr_float_value] + [arr_float_value]", from_=data_table) == dbe.eval(
                "ARRAY(0, 45, 0.123, NULL, 0, 45, 0.123, NULL)"
            )
            assert dbe.eval("ARRAY(1.1, 2.2, 3.3) + ARRAY(6.6, NULL, 0)", from_=data_table) == dbe.eval(
                "ARRAY(1.1, 2.2, 3.3, 6.6, NULL, 0)"
            )
