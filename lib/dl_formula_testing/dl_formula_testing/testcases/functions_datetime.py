from __future__ import annotations

import datetime
from typing import (
    Any,
    ClassVar,
)

import pytest
import pytz
import sqlalchemy.exc as sa_exc

from dl_formula.core.exc import TranslationError
from dl_formula_testing.evaluator import DbEvaluator
from dl_formula_testing.testcases.base import FormulaConnectorTestBase
from dl_formula_testing.util import (
    approx_datetime,
    dt_strip,
    now,
)


def int_value(value: Any) -> int:
    assert isinstance(value, int)
    return value


class DefaultDateTimeFunctionFormulaConnectorTestSuite(FormulaConnectorTestBase):
    supports_addition_to_feb_29: ClassVar[bool] = True
    supports_dateadd_non_const_unit_num: ClassVar[bool] = False
    supports_deprecated_dateadd: ClassVar[bool] = False
    supports_deprecated_datepart_2: ClassVar[bool] = False
    supports_datepart_2_non_const: ClassVar[bool] = True
    supports_datetrunc_3: ClassVar[bool] = False
    supports_datetimetz: ClassVar[bool] = False

    @pytest.fixture(name="now")
    def fixture_now(self, dbe: DbEvaluator) -> datetime.datetime:
        return now().replace(tzinfo=pytz.UTC).astimezone(dbe.db.tzinfo).replace(tzinfo=None)

    @pytest.fixture(name="today")
    def fixture_today(self, now: datetime.datetime) -> datetime.date:
        return now.date()

    @pytest.mark.parametrize("func_name", ("NOW", "GENERICNOW"))
    def test_now(self, dbe: DbEvaluator, func_name: str, now: datetime.datetime) -> None:
        assert dt_strip(dbe.eval(f"{func_name}()")) == approx_datetime(now)

    def test_today(self, dbe: DbEvaluator, today: datetime.date) -> None:
        assert dbe.eval("TODAY()") == today

    def test_dateadd_to_now(self, dbe: DbEvaluator, now: datetime.datetime) -> None:
        assert dt_strip(dbe.eval('DATEADD(NOW(), "day", 1)')) == approx_datetime(now + datetime.timedelta(days=1))

    def test_dateadd_to_today(self, dbe: DbEvaluator, today: datetime.date) -> None:
        assert dbe.eval('DATEADD(TODAY(), "day", 1)') == today + datetime.timedelta(days=1)

    def test_datetime_dateadd_with_uneven_month_lengths(self, dbe: DbEvaluator) -> None:
        if not self.supports_addition_to_feb_29:
            with pytest.raises(sa_exc.DatabaseError):
                assert dbe.eval('DATEADD(#2020-02-29#, "year", 1)')

        else:
            # Leap year
            assert dbe.eval('DATEADD(#2020-02-29#, "year", 1)') == datetime.date(2021, 2, 28)
            assert dbe.eval('DATEADD(#2020-02-29#, "year", -1)') == datetime.date(2019, 2, 28)
            # Short and long months
            assert dbe.eval('DATEADD(#2021-01-30#, "month", 1)') == datetime.date(2021, 2, 28)
            assert dbe.eval('DATEADD(#2021-03-30#, "month", -1)') == datetime.date(2021, 2, 28)

    @pytest.mark.parametrize("lit", (pytest.param("##", id="generic"), pytest.param("#", id="regular")))
    def test_datetime_dateadd(self, dbe: DbEvaluator, lit: str) -> None:
        def _dt_lit(s: str) -> str:
            return f"{lit}{s}{lit}"

        # 1 arg
        assert dbe.eval("DATEADD(#2018-01-12#)") == datetime.date(2018, 1, 13)

        # 2 args (unit)
        assert dbe.eval('DATEADD(#2018-01-12#, "day")') == datetime.date(2018, 1, 13)
        assert dbe.eval('DATEADD(#2018-01-12#, "month")') == datetime.date(2018, 2, 12)
        assert dbe.eval('DATEADD(#2018-01-12#, "quarter")') == datetime.date(2018, 4, 12)
        assert dbe.eval('DATEADD(#2018-01-12#, "year")') == datetime.date(2019, 1, 12)

        # 2 args (number)
        assert dbe.eval("DATEADD(#2018-01-12#, 6)") == datetime.date(2018, 1, 18)

        # 3 args
        assert dbe.eval('DATEADD(#2018-01-12#, "day", 6)') == datetime.date(2018, 1, 18)
        assert dbe.eval('DATEADD(#2018-01-12#, "month", 6)') == datetime.date(2018, 7, 12)
        assert dbe.eval('DATEADD(#2018-01-12#, "quarter", 6)') == datetime.date(2019, 7, 12)
        assert dbe.eval('DATEADD(#2018-01-12#, "year", 6)') == datetime.date(2024, 1, 12)
        assert dt_strip(dbe.eval(f'DATEADD({_dt_lit("2018-01-12 01:02:03")}, "second", 6)')) == datetime.datetime(
            2018, 1, 12, 1, 2, 9
        )
        assert dt_strip(dbe.eval(f'DATEADD({_dt_lit("2018-01-12 01:02:03")}, "minute", 6)')) == datetime.datetime(
            2018, 1, 12, 1, 8, 3
        )
        assert dt_strip(dbe.eval(f'DATEADD({_dt_lit("2018-01-12 01:02:03")}, "hour", 6)')) == datetime.datetime(
            2018, 1, 12, 7, 2, 3
        )
        assert dt_strip(dbe.eval(f'DATEADD({_dt_lit("2018-01-12 01:02:03")}, "day", 6)')) == datetime.datetime(
            2018, 1, 18, 1, 2, 3
        )
        assert dt_strip(dbe.eval(f'DATEADD({_dt_lit("2018-01-12 01:02:03")}, "month", 6)')) == datetime.datetime(
            2018, 7, 12, 1, 2, 3
        )
        assert dt_strip(dbe.eval(f'DATEADD({_dt_lit("2018-01-12 01:02:03")}, "quarter", 6)')) == datetime.datetime(
            2019, 7, 12, 1, 2, 3
        )
        assert dt_strip(dbe.eval(f'DATEADD({_dt_lit("2018-01-12 01:02:03")}, "year", 6)')) == datetime.datetime(
            2024, 1, 12, 1, 2, 3
        )
        if self.supports_dateadd_non_const_unit_num:
            # non-const number
            assert dbe.eval('DATEADD(#2018-01-12#, "day", INT("6"))') == datetime.date(2018, 1, 18)
            assert dbe.eval('DATEADD(#2018-01-12#, "month", INT("6"))') == datetime.date(2018, 7, 12)
            assert dbe.eval('DATEADD(#2018-01-12#, "quarter", INT("6"))') == datetime.date(2019, 7, 12)
            assert dbe.eval('DATEADD(#2018-01-12#, "year", INT("6"))') == datetime.date(2024, 1, 12)
            assert dt_strip(
                dbe.eval(f'DATEADD({_dt_lit("2018-01-12 01:02:03")}, "second", INT("6"))')
            ) == datetime.datetime(2018, 1, 12, 1, 2, 9)
            assert dt_strip(
                dbe.eval(f'DATEADD({_dt_lit("2018-01-12 01:02:03")}, "minute", INT("6"))')
            ) == datetime.datetime(2018, 1, 12, 1, 8, 3)
            assert dt_strip(
                dbe.eval(f'DATEADD({_dt_lit("2018-01-12 01:02:03")}, "hour", INT("6"))')
            ) == datetime.datetime(2018, 1, 12, 7, 2, 3)
            assert dt_strip(
                dbe.eval(f'DATEADD({_dt_lit("2018-01-12 01:02:03")}, "day", INT("6"))')
            ) == datetime.datetime(2018, 1, 18, 1, 2, 3)
            assert dt_strip(
                dbe.eval(f'DATEADD({_dt_lit("2018-01-12 01:02:03")}, "month", INT("6"))')
            ) == datetime.datetime(2018, 7, 12, 1, 2, 3)
            assert dt_strip(
                dbe.eval(f'DATEADD({_dt_lit("2018-01-12 01:02:03")}, "quarter", INT("6"))')
            ) == datetime.datetime(2019, 7, 12, 1, 2, 3)
            assert dt_strip(
                dbe.eval(f'DATEADD({_dt_lit("2018-01-12 01:02:03")}, "year", INT("6"))')
            ) == datetime.datetime(2024, 1, 12, 1, 2, 3)

    @pytest.mark.parametrize("lit", (pytest.param("##", id="generic"), pytest.param("#", id="regular")))
    def test_datetime_dateadd_deprecated(self, dbe: DbEvaluator, lit: str) -> None:
        if not self.supports_deprecated_dateadd:
            pytest.skip()

        def _dt_lit(s: str) -> str:
            return f"{lit}{s}{lit}"

        assert dbe.eval('DATEADD("day", 6, #2018-01-12#)') == datetime.date(2018, 1, 18)
        assert dbe.eval('DATEADD("month", 6, #2018-01-12#)') == datetime.date(2018, 7, 12)
        assert dbe.eval('DATEADD("quarter", 6, #2018-01-12#)') == datetime.date(2019, 7, 12)
        assert dbe.eval('DATEADD("year", 6, #2018-01-12#)') == datetime.date(2024, 1, 12)
        assert dt_strip(dbe.eval(f'DATEADD("second", 6, {_dt_lit("2018-01-12 01:02:03")})')) == datetime.datetime(
            2018, 1, 12, 1, 2, 9
        )
        assert dt_strip(dbe.eval(f'DATEADD("minute", 6, {_dt_lit("2018-01-12 01:02:03")})')) == datetime.datetime(
            2018, 1, 12, 1, 8, 3
        )
        assert dt_strip(dbe.eval(f'DATEADD("hour", 6, {_dt_lit("2018-01-12 01:02:03")})')) == datetime.datetime(
            2018, 1, 12, 7, 2, 3
        )
        assert dt_strip(dbe.eval(f'DATEADD("day", 6, {_dt_lit("2018-01-12 01:02:03")})')) == datetime.datetime(
            2018, 1, 18, 1, 2, 3
        )
        assert dt_strip(dbe.eval(f'DATEADD("month", 6, {_dt_lit("2018-01-12 01:02:03")})')) == datetime.datetime(
            2018, 7, 12, 1, 2, 3
        )
        assert dt_strip(dbe.eval(f'DATEADD("quarter", 6, {_dt_lit("2018-01-12 01:02:03")})')) == datetime.datetime(
            2019, 7, 12, 1, 2, 3
        )
        assert dt_strip(dbe.eval(f'DATEADD("year", 6, {_dt_lit("2018-01-12 01:02:03")})')) == datetime.datetime(
            2024, 1, 12, 1, 2, 3
        )

    def test_datepart_2_const(self, dbe: DbEvaluator, forced_literal_use: Any) -> None:
        # const version
        # date
        assert int_value(dbe.eval('DATEPART(#2018-01-12#, "second")')) == 0
        assert int_value(dbe.eval('DATEPART(#2018-01-12#, "minute")')) == 0
        assert int_value(dbe.eval('DATEPART(#2018-01-12#, "hour")')) == 0
        assert int_value(dbe.eval('DATEPART(#2018-01-12#, "day")')) == 12
        assert int_value(dbe.eval('DATEPART(#2018-01-12#, "month")')) == 1
        assert int_value(dbe.eval('DATEPART(#2018-01-12#, "quarter")')) == 1
        assert int_value(dbe.eval('DATEPART(#2018-01-12#, "year")')) == 2018
        assert int_value(dbe.eval('DATEPART(#1971-01-14#, "dayofweek")')) == 4
        assert int_value(dbe.eval('DATEPART(#1971-01-14#, "dow")')) == 4
        assert int_value(dbe.eval('DATEPART(#1971-01-14#, "week")')) == 2

        # datetime
        assert int_value(dbe.eval('DATEPART(#2018-01-12 01:02:03#, "second")')) == 3
        assert int_value(dbe.eval('DATEPART(#2018-01-12 01:02:03#, "minute")')) == 2
        assert int_value(dbe.eval('DATEPART(#2018-01-12 01:02:03#, "hour")')) == 1
        assert int_value(dbe.eval('DATEPART(#2018-01-12 01:02:03#, "day")')) == 12
        assert int_value(dbe.eval('DATEPART(#2018-01-12 01:02:03#, "month")')) == 1
        assert int_value(dbe.eval('DATEPART(#2018-01-12 01:02:03#, "quarter")')) == 1
        assert int_value(dbe.eval('DATEPART(#2018-01-12 01:02:03#, "year")')) == 2018
        assert int_value(dbe.eval('DATEPART(#1971-01-14 01:02:03#, "dayofweek")')) == 4
        assert int_value(dbe.eval('DATEPART(#1971-01-14 01:02:03#, "dow")')) == 4
        assert int_value(dbe.eval('DATEPART(#1971-01-14 01:02:03#, "week")')) == 2

    def test_datepart_3(self, dbe: DbEvaluator, forced_literal_use: Any) -> None:
        assert dbe.eval('DATEPART(#1971-01-14 01:02:03#, "dayofweek", "mon")') == 4
        assert dbe.eval('DATEPART(#1971-01-14 01:02:03#, "dow", "mon")') == 4
        assert dbe.eval('DATEPART(#1971-01-14 01:02:03#, "dayofweek", "wed")') == 2
        assert dbe.eval('DATEPART(#1971-01-14 01:02:03#, "dow", "wed")') == 2
        assert dbe.eval('DATEPART(#1971-01-14 01:02:03#, "dayofweek", "wednesday")') == 2

    def test_datepart_2_non_const(self, dbe: DbEvaluator, forced_literal_use: Any) -> None:
        if not self.supports_datepart_2_non_const:
            pytest.skip()

        # date
        assert int_value(dbe.eval('DATEPART(#2018-01-12#, __LIT__("second"))')) == 0
        assert int_value(dbe.eval('DATEPART(#2018-01-12#, __LIT__("minute"))')) == 0
        assert int_value(dbe.eval('DATEPART(#2018-01-12#, __LIT__("hour"))')) == 0
        assert int_value(dbe.eval('DATEPART(#2018-01-12#, __LIT__("day"))')) == 12
        assert int_value(dbe.eval('DATEPART(#2018-01-12#, __LIT__("month"))')) == 1
        assert int_value(dbe.eval('DATEPART(#2018-01-12#, __LIT__("quarter"))')) == 1
        assert int_value(dbe.eval('DATEPART(#2018-01-12#, __LIT__("year"))')) == 2018
        assert int_value(dbe.eval('DATEPART(#1971-01-14#, __LIT__("dayofweek"))')) == 4
        assert int_value(dbe.eval('DATEPART(#1971-01-14#, __LIT__("dow"))')) == 4
        assert int_value(dbe.eval('DATEPART(#1971-01-14#, __LIT__("week"))')) == 2

        # datetime
        assert int_value(dbe.eval('DATEPART(#2018-01-12 01:02:03#, __LIT__("second"))')) == 3
        assert int_value(dbe.eval('DATEPART(#2018-01-12 01:02:03#, __LIT__("minute"))')) == 2
        assert int_value(dbe.eval('DATEPART(#2018-01-12 01:02:03#, __LIT__("hour"))')) == 1
        assert int_value(dbe.eval('DATEPART(#2018-01-12 01:02:03#, __LIT__("day"))')) == 12
        assert int_value(dbe.eval('DATEPART(#2018-01-12 01:02:03#, __LIT__("month"))')) == 1
        assert int_value(dbe.eval('DATEPART(#2018-01-12 01:02:03#, __LIT__("quarter"))')) == 1
        assert int_value(dbe.eval('DATEPART(#2018-01-12 01:02:03#, __LIT__("year"))')) == 2018
        assert int_value(dbe.eval('DATEPART(#1971-01-14 01:02:03#, __LIT__("dayofweek"))')) == 4
        assert int_value(dbe.eval('DATEPART(#1971-01-14 01:02:03#, __LIT__("dow"))')) == 4
        assert int_value(dbe.eval('DATEPART(#1971-01-14 01:02:03#, __LIT__("week"))')) == 2

    def test_datepart_2_deprecated(self, dbe: DbEvaluator, forced_literal_use: Any) -> None:
        if not self.supports_deprecated_datepart_2:
            pytest.skip()

        # const
        assert dbe.eval('DATEPART("second", #2018-01-12#)') == 0
        assert dbe.eval('DATEPART("minute", #2018-01-12#)') == 0
        assert dbe.eval('DATEPART("hour", #2018-01-12#)') == 0
        assert dbe.eval('DATEPART("day", #2018-01-12#)') == 12
        assert dbe.eval('DATEPART("month", #2018-01-12#)') == 1
        assert dbe.eval('DATEPART("quarter", #2018-01-12#)') == 1
        assert dbe.eval('DATEPART("year", #2018-01-12#)') == 2018
        assert dbe.eval('DATEPART("dayofweek", #1971-01-14#)') == 4
        assert dbe.eval('DATEPART("dow", #1971-01-14#)') == 4
        assert dbe.eval('DATEPART("week", #1971-01-14#)') == 2
        assert dbe.eval('DATEPART("second", #2018-01-12 01:02:03#)') == 3
        assert dbe.eval('DATEPART("minute", #2018-01-12 01:02:03#)') == 2
        assert dbe.eval('DATEPART("hour", #2018-01-12 01:02:03#)') == 1
        assert dbe.eval('DATEPART("day", #2018-01-12 01:02:03#)') == 12
        assert dbe.eval('DATEPART("month", #2018-01-12 01:02:03#)') == 1
        assert dbe.eval('DATEPART("quarter", #2018-01-12 01:02:03#)') == 1
        assert dbe.eval('DATEPART("year", #2018-01-12 01:02:03#)') == 2018
        assert dbe.eval('DATEPART("dayofweek", #1971-01-14 01:02:03#)') == 4
        assert dbe.eval('DATEPART("dow", #1971-01-14 01:02:03#)') == 4
        assert dbe.eval('DATEPART("week", #1971-01-14 01:02:03#)') == 2

        # non-const
        assert dbe.eval('DATEPART(__LIT__("second"), #2018-01-12#)') == 0
        assert dbe.eval('DATEPART(__LIT__("minute"), #2018-01-12#)') == 0
        assert dbe.eval('DATEPART(__LIT__("hour"), #2018-01-12#)') == 0
        assert dbe.eval('DATEPART(__LIT__("day"), #2018-01-12#)') == 12
        assert dbe.eval('DATEPART(__LIT__("month"), #2018-01-12#)') == 1
        assert dbe.eval('DATEPART(__LIT__("quarter"), #2018-01-12#)') == 1
        assert dbe.eval('DATEPART(__LIT__("year"), #2018-01-12#)') == 2018
        assert dbe.eval('DATEPART(__LIT__("dayofweek"), #1971-01-14#)') == 4
        assert dbe.eval('DATEPART(__LIT__("dow"), #1971-01-14#)') == 4
        assert dbe.eval('DATEPART(__LIT__("week"), #1971-01-14#)') == 2
        assert dbe.eval('DATEPART(__LIT__("second"), #2018-01-12 01:02:03#)') == 3
        assert dbe.eval('DATEPART(__LIT__("minute"), #2018-01-12 01:02:03#)') == 2
        assert dbe.eval('DATEPART(__LIT__("hour"), #2018-01-12 01:02:03#)') == 1
        assert dbe.eval('DATEPART(__LIT__("day"), #2018-01-12 01:02:03#)') == 12
        assert dbe.eval('DATEPART(__LIT__("month"), #2018-01-12 01:02:03#)') == 1
        assert dbe.eval('DATEPART(__LIT__("quarter"), #2018-01-12 01:02:03#)') == 1
        assert dbe.eval('DATEPART(__LIT__("year"), #2018-01-12 01:02:03#)') == 2018
        assert dbe.eval('DATEPART(__LIT__("dayofweek"), #1971-01-14 01:02:03#)') == 4
        assert dbe.eval('DATEPART(__LIT__("dow"), #1971-01-14 01:02:03#)') == 4
        assert dbe.eval('DATEPART(__LIT__("week"), #1971-01-14 01:02:03#)') == 2

    def test_specific_date_part_functions(self, dbe: DbEvaluator) -> None:
        assert int_value(dbe.eval("SECOND(#2018-01-12#)")) == 0
        assert int_value(dbe.eval("MINUTE(#2018-01-12#)")) == 0
        assert int_value(dbe.eval("HOUR(#2018-01-12#)")) == 0
        assert int_value(dbe.eval("DAY(#2018-01-12#)")) == 12
        assert int_value(dbe.eval("MONTH(#2018-01-12#)")) == 1
        assert int_value(dbe.eval("QUARTER(#2018-01-12#)")) == 1
        assert int_value(dbe.eval("YEAR(#2018-01-12#)")) == 2018
        assert int_value(dbe.eval("DAYOFWEEK(#1971-01-14#)")) == 4
        assert int_value(dbe.eval("WEEK(#1971-01-14#)")) == 2
        assert int_value(dbe.eval("SECOND(#2018-01-12 01:02:03#)")) == 3
        assert int_value(dbe.eval("MINUTE(#2018-01-12 01:02:03#)")) == 2
        assert int_value(dbe.eval("HOUR(#2018-01-12 01:02:03#)")) == 1
        assert int_value(dbe.eval("DAY(#2018-01-12 01:02:03#)")) == 12
        assert int_value(dbe.eval("MONTH(#2018-01-12 01:02:03#)")) == 1
        assert int_value(dbe.eval("QUARTER(#2018-01-12 01:02:03#)")) == 1
        assert int_value(dbe.eval("YEAR(#2018-01-12 01:02:03#)")) == 2018
        assert int_value(dbe.eval("DAYOFWEEK(#1971-01-14 01:02:03#)")) == 4
        assert int_value(dbe.eval("WEEK(#1971-01-14 01:02:03#)")) == 2

        assert int_value(dbe.eval('DAYOFWEEK(#1971-01-14#, "wed")')) == 2
        assert int_value(dbe.eval('DAYOFWEEK(#1971-01-14#, "wednesday")')) == 2

    def test_datetrunc_2(self, dbe: DbEvaluator) -> None:
        assert dbe.eval('DATETRUNC(#2018-07-12#, "second")') == datetime.date(2018, 7, 12)
        assert dbe.eval('DATETRUNC(#2018-07-12#, "minute")') == datetime.date(2018, 7, 12)
        assert dbe.eval('DATETRUNC(#2018-07-12#, "hour")') == datetime.date(2018, 7, 12)
        assert dbe.eval('DATETRUNC(#2018-07-12#, "day")') == datetime.date(2018, 7, 12)
        assert dbe.eval('DATETRUNC(#2018-07-12#, "week")') == datetime.date(2018, 7, 9)
        # Sic! This is exactly how ISO8601 works for week
        assert dbe.eval('DATETRUNC(#2015-01-01#, "week")') == datetime.date(2014, 12, 29)
        assert dbe.eval('DATETRUNC(#2017-01-01#, "week")') == datetime.date(2016, 12, 26)
        assert dbe.eval('DATETRUNC(#2017-01-02#, "week")') == datetime.date(2017, 1, 2)
        assert dbe.eval('DATETRUNC(#2019-01-01#, "week")') == datetime.date(2018, 12, 31)
        assert dbe.eval('DATETRUNC(#2019-12-31#, "week")') == datetime.date(2019, 12, 30)
        assert dbe.eval('DATETRUNC(#2020-01-01#, "week")') == datetime.date(2019, 12, 30)
        assert dbe.eval('DATETRUNC(#2021-01-01#, "week")') == datetime.date(2020, 12, 28)
        assert dbe.eval('DATETRUNC(#2021-01-02#, "week")') == datetime.date(2020, 12, 28)
        assert dbe.eval('DATETRUNC(#2021-01-03#, "week")') == datetime.date(2020, 12, 28)
        assert dbe.eval('DATETRUNC(#2021-01-04#, "week")') == datetime.date(2021, 1, 4)
        assert dbe.eval('DATETRUNC(#2021-01-07#, "week")') == datetime.date(2021, 1, 4)
        assert dbe.eval('DATETRUNC(#2022-01-01#, "week")') == datetime.date(2021, 12, 27)
        assert dbe.eval('DATETRUNC(#2023-01-01#, "week")') == datetime.date(2022, 12, 26)
        assert dbe.eval('DATETRUNC(#2018-07-12#, "month")') == datetime.date(2018, 7, 1)
        assert dbe.eval('DATETRUNC(#2018-07-12#, "quarter")') == datetime.date(2018, 7, 1)
        assert dbe.eval('DATETRUNC(#2018-08-12#, "quarter")') == datetime.date(2018, 7, 1)
        assert dbe.eval('DATETRUNC(#2018-09-12#, "quarter")') == datetime.date(2018, 7, 1)
        assert dbe.eval('DATETRUNC(#2018-07-12#, "year")') == datetime.date(2018, 1, 1)
        with pytest.raises(TranslationError):
            dbe.eval('DATETRUNC(#2018-07-12#, "unexpected")')
        assert dt_strip(dbe.eval('DATETRUNC(#2018-07-12 11:07:13#, "second")')) == datetime.datetime(
            2018, 7, 12, 11, 7, 13
        )
        assert dt_strip(dbe.eval('DATETRUNC(#2018-01-02 03:04:05#, "second")')) == datetime.datetime(
            2018, 1, 2, 3, 4, 5
        )
        assert dt_strip(dbe.eval('DATETRUNC(#2018-07-12 11:07:13#, "minute")')) == datetime.datetime(
            2018, 7, 12, 11, 7, 0
        )
        assert dt_strip(dbe.eval('DATETRUNC(#2018-01-02 03:04:05#, "minute")')) == datetime.datetime(
            2018, 1, 2, 3, 4, 0
        )
        assert dt_strip(dbe.eval('DATETRUNC(#2018-07-12 11:07:13#, "hour")')) == datetime.datetime(
            2018, 7, 12, 11, 0, 0
        )
        assert dt_strip(dbe.eval('DATETRUNC(#2018-01-02 03:04:05#, "hour")')) == datetime.datetime(2018, 1, 2, 3, 0, 0)
        assert dt_strip(dbe.eval('DATETRUNC(#2022-10-01 15:21:40#, "hour")')) == datetime.datetime(
            2022, 10, 1, 15, 0, 0
        )
        assert dt_strip(dbe.eval('DATETRUNC(#2022-09-19 15:21:40#, "hour")')) == datetime.datetime(
            2022, 9, 19, 15, 0, 0
        )
        assert dt_strip(dbe.eval('DATETRUNC(#2018-07-12 11:07:13#, "day")')) == datetime.datetime(2018, 7, 12, 0, 0, 0)
        assert dt_strip(dbe.eval('DATETRUNC(#2018-01-02 03:04:05#, "day")')) == datetime.datetime(2018, 1, 2, 0, 0, 0)
        assert dt_strip(dbe.eval('DATETRUNC(#2018-07-12 11:07:13#, "week")')) == datetime.datetime(2018, 7, 9, 0, 0, 0)
        assert dt_strip(dbe.eval('DATETRUNC(#2018-01-02 03:04:05#, "week")')) == datetime.datetime(2018, 1, 1, 0, 0, 0)
        assert dt_strip(dbe.eval('DATETRUNC(#2018-07-12 11:07:13#, "month")')) == datetime.datetime(2018, 7, 1, 0, 0, 0)
        assert dt_strip(dbe.eval('DATETRUNC(#2018-01-02 03:04:05#, "month")')) == datetime.datetime(2018, 1, 1, 0, 0, 0)
        assert dt_strip(dbe.eval('DATETRUNC(#2018-07-12 11:07:13#, "quarter")')) == datetime.datetime(
            2018, 7, 1, 0, 0, 0
        )
        assert dt_strip(dbe.eval('DATETRUNC(#2018-07-12 11:07:13#, "year")')) == datetime.datetime(2018, 1, 1, 0, 0, 0)
        with pytest.raises(TranslationError):
            dbe.eval('DATETRUNC(#2018-07-12 11:07:13#, "unexpected")')

    def test_datetrunc_3(self, dbe: DbEvaluator) -> None:
        if not self.supports_datetrunc_3:
            pytest.skip()

        assert dbe.eval('DATETRUNC(#2018-07-12#, "second", 5)') == datetime.date(2018, 7, 12)
        assert dbe.eval('DATETRUNC(#2018-07-12#, "minute", 5)') == datetime.date(2018, 7, 12)
        # assert dbe.eval('DATETRUNC(#2018-07-12#, "hour", 5)') == datetime.date(2018, 7, 12)  # FIXME: https://github.com/ClickHouse/ClickHouse/issues/10124
        assert dbe.eval('DATETRUNC(#2018-07-12#, "day", 5)') == datetime.date(2018, 7, 8)
        assert dbe.eval('DATETRUNC(#2018-07-12#, "week", 5)') == datetime.date(2018, 7, 2)
        assert dbe.eval('DATETRUNC(#2018-07-12#, "month", 4)') == datetime.date(2018, 5, 1)
        assert dbe.eval('DATETRUNC(#2018-07-12#, "quarter", 2)') == datetime.date(2018, 7, 1)
        assert dbe.eval('DATETRUNC(#2018-07-12#, "year", 5)') == datetime.date(2015, 1, 1)
        with pytest.raises(TranslationError):
            dbe.eval('DATETRUNC(#2018-07-12#, "unexpected", 5)')
        assert dt_strip(dbe.eval('DATETRUNC(#2018-07-12 11:07:13#, "second", 5)')) == datetime.datetime(
            2018, 7, 12, 11, 7, 10
        )
        assert dt_strip(dbe.eval('DATETRUNC(#2018-07-12 11:07:13#, "minute", 5)')) == datetime.datetime(
            2018, 7, 12, 11, 5, 0
        )
        # assert dt_strip(  # FIXME: https://github.com/ClickHouse/ClickHouse/issues/10124
        #     dbe.eval('DATETRUNC(#2018-07-12 11:07:13#, "hour", 5)')) == datetime.datetime(2018, 7, 12, 10, 0, 0)
        assert dt_strip(dbe.eval('DATETRUNC(#2018-07-12 11:07:13#, "day", 5)')) == datetime.datetime(
            2018, 7, 8, 0, 0, 0
        )
        assert dt_strip(dbe.eval('DATETRUNC(#2018-07-12 11:07:13#, "week", 5)')) == datetime.datetime(
            2018, 7, 2, 0, 0, 0
        )
        assert dt_strip(dbe.eval('DATETRUNC(#2018-07-12 11:07:13#, "month", 4)')) == datetime.datetime(
            2018, 5, 1, 0, 0, 0
        )
        assert dt_strip(dbe.eval('DATETRUNC(#2018-07-12 11:07:13#, "quarter", 2)')) == datetime.datetime(
            2018, 7, 1, 0, 0, 0
        )
        assert dt_strip(dbe.eval('DATETRUNC(#2018-07-12 11:07:13#, "year", 5)')) == datetime.datetime(
            2015, 1, 1, 0, 0, 0
        )
        with pytest.raises(TranslationError):
            dbe.eval('DATETRUNC(#2018-07-12 11:07:13#, "unexpected", 5)')

    @pytest.mark.parametrize(
        "value_expr",
        (
            "__LIT__('2019-01-01T00:01:02')",
            "'2019-01-01T00:01:02'",
            "#2019-01-01T00:01:02#",
        ),
    )
    def test_datetimetz_make(self, dbe: DbEvaluator, forced_literal_use: Any, value_expr: str) -> None:
        if not self.supports_datetimetz:
            pytest.skip()

        tz = "America/New_York"
        expr = f"DATETIMETZ({value_expr}, '{tz}')"
        resp = dbe.eval(expr)

        assert isinstance(resp, datetime.datetime), resp
        assert resp.tzinfo, resp

        # -05:00, so +5hrs at UTC
        expected = datetime.datetime(2019, 1, 1, 5, 1, 2)
        assert resp.astimezone(datetime.timezone.utc).replace(tzinfo=None) == expected

    @pytest.fixture(params=["const", "lit"])
    def dttz_expr(self, request: Any, dbe: DbEvaluator, forced_literal_use: Any) -> str:
        if not self.supports_datetimetz:
            pytest.skip()

        value_iso = "2019-01-01T00:01:02"
        tz = "America/New_York"
        expr = f"'{value_iso}'"
        if request.param == "lit":
            expr = f"__LIT__({expr})"
        expr = f"DATETIMETZ({expr}, '{tz}')"
        return expr

    def test_datetimetz_funcs(self, dbe: DbEvaluator, dttz_expr: str, forced_literal_use: Any) -> None:
        resp = dbe.eval(f"AVG({dttz_expr})")
        assert isinstance(resp, datetime.datetime) and resp.tzinfo
        resp = dbe.eval(f"MAX({dttz_expr})")
        assert isinstance(resp, datetime.datetime) and resp.tzinfo
        resp = dbe.eval(f"MIN({dttz_expr})")
        assert isinstance(resp, datetime.datetime) and resp.tzinfo
        resp = dbe.eval(f"DATEADD({dttz_expr}, 'month', 2)")
        assert isinstance(resp, datetime.datetime) and resp.tzinfo
        assert dbe.eval(f"DATEPART({dttz_expr}, 'dayofweek', 'tue')") == 1
        assert dbe.eval(f"DATEPART({dttz_expr}, __LIT__('dayofweek'), 'wed')") == 7
        assert dbe.eval(f"DAYOFWEEK({dttz_expr})") == 2
        assert dbe.eval(f"DAYOFWEEK({dttz_expr}, 'thu')") == 6
        resp = dbe.eval(f"DATETRUNC({dttz_expr}, 'hour')")
        assert isinstance(resp, datetime.datetime) and resp.tzinfo
        resp = dbe.eval(f"DATETRUNC({dttz_expr}, 'month')")
        assert isinstance(resp, datetime.datetime) and resp.tzinfo

        if self.supports_datetrunc_3:
            resp = dbe.eval(f"DATETRUNC({dttz_expr}, 'month', 3)")
            assert isinstance(resp, datetime.datetime) and resp.tzinfo

        resp = dbe.eval(f"DATETRUNC({dttz_expr}, 'quarter')")
        assert isinstance(resp, datetime.datetime) and resp.tzinfo
        assert dbe.eval(f"DATE({dttz_expr})") == datetime.date(2019, 1, 1)

        assert dbe.eval(f"SECOND({dttz_expr})") == 2
        assert dbe.eval(f"MINUTE({dttz_expr})") == 1
        assert dbe.eval(f"HOUR({dttz_expr})") == 0
        assert dbe.eval(f"DAY({dttz_expr})") == 1
        assert dbe.eval(f"WEEK({dttz_expr})") == 1
        assert dbe.eval(f"MONTH({dttz_expr})") == 1
        assert dbe.eval(f"QUARTER({dttz_expr})") == 1
        assert dbe.eval(f"YEAR({dttz_expr})") == 2019

        # TODO: UTCNOW()
        # TODO: UTCTODAY()
        resp = dbe.eval(f"GREATEST({dttz_expr}, {dttz_expr})")
        assert isinstance(resp, datetime.datetime) and resp.tzinfo
        resp = dbe.eval(f"GREATEST({dttz_expr}, {dttz_expr}, {dttz_expr}, {dttz_expr})")
        assert isinstance(resp, datetime.datetime) and resp.tzinfo
        resp = dbe.eval(f"LEAST({dttz_expr}, {dttz_expr})")
        assert isinstance(resp, datetime.datetime) and resp.tzinfo
        with pytest.raises(TranslationError):
            dbe.eval(f"GREATEST({dttz_expr}, #2018-07-12 11:07:13#)")
        with pytest.raises(TranslationError):
            dbe.eval(f"LEAST(#2018-07-12 11:07:13#, {dttz_expr})")
        with pytest.raises(TranslationError):
            dbe.eval(f"LEAST({dttz_expr}, {dttz_expr}, #2018-07-12 11:07:13#, {dttz_expr}, {dttz_expr})")
        resp = dbe.eval(f"CONCAT('at ', {dttz_expr}, ' there')")
        assert isinstance(resp, str)
