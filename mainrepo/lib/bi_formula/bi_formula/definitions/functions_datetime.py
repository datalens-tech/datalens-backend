from __future__ import annotations

import sqlalchemy as sa

from bi_formula.core.datatype import DataType
from bi_formula.core.dialect import StandardDialect as D
import bi_formula.core.exc as exc
from bi_formula.definitions.args import ArgTypeSequence
from bi_formula.definitions.base import (
    Function,
    TranslationVariant,
    TranslationVariantWrapped,
)
from bi_formula.definitions.common_datetime import (
    ensure_naive_first_arg,
    normalize_and_validate_datetime_interval_type,
)
from bi_formula.definitions.flags import ContextFlag
from bi_formula.definitions.literals import un_literal
from bi_formula.definitions.scope import Scope
from bi_formula.definitions.type_strategy import (
    Fixed,
    FromArgs,
    ParamsFromArgs,
)
from bi_formula.shortcuts import n

V = TranslationVariant.make
VW = TranslationVariantWrapped.make

_DATETIME_TYPES = {DataType.DATETIME, DataType.DATETIMETZ, DataType.GENERICDATETIME}


class DateFunction(Function):
    pass


class FuncDateaddBase(DateFunction):
    name = "dateadd"
    arg_names = ["datetime", "unit", "number"]
    return_type = FromArgs(0)
    return_type_params = ParamsFromArgs(0)


class FuncDateadd1Base(FuncDateaddBase):
    arg_cnt = 1


class FuncDateadd1(FuncDateadd1Base):
    variants = [
        VW(D.DUMMY | D.SQLITE, lambda date: n.func.DATEADD(date, "day", 1)),
    ]
    argument_types = [
        ArgTypeSequence([{DataType.DATE, *_DATETIME_TYPES}]),
    ]


class FuncDateadd2Base(FuncDateaddBase):
    arg_cnt = 2


class FuncDateadd2Unit(FuncDateadd2Base):
    variants = [
        VW(D.DUMMY | D.SQLITE, lambda date, unit: n.func.DATEADD(date, unit, 1)),
    ]
    argument_types = [
        ArgTypeSequence([{DataType.DATE, *_DATETIME_TYPES}, DataType.CONST_STRING]),
    ]


class FuncDateadd2Number(FuncDateadd2Base):
    variants = [
        VW(D.DUMMY | D.SQLITE, lambda date, number: n.func.DATEADD(date, "day", number)),
    ]
    argument_types = [
        ArgTypeSequence([{DataType.DATE, *_DATETIME_TYPES}, DataType.CONST_INTEGER]),
    ]


class FuncDateadd3Base(FuncDateaddBase):
    arg_cnt = 3


class FuncDateadd3Legacy(FuncDateadd3Base):
    """Legacy version of the function with `date` as the last argument"""

    variants = [VW(D.DUMMY | D.SQLITE, lambda what, num, date: n.func.DATEADD(date, what, num))]
    argument_types = [
        ArgTypeSequence([DataType.STRING, DataType.INTEGER, DataType.DATE]),
        ArgTypeSequence([DataType.STRING, DataType.INTEGER, DataType.DATETIME]),
        ArgTypeSequence([DataType.STRING, DataType.INTEGER, DataType.GENERICDATETIME]),
    ]
    return_type = FromArgs(2)
    return_flags = ContextFlag.DEPRECATED


class FuncDateadd3DateConstNum(FuncDateadd3Base):
    argument_types = [
        ArgTypeSequence([DataType.DATE, DataType.CONST_STRING, DataType.CONST_INTEGER]),
    ]


class FuncDateadd3DateNonConstNum(FuncDateadd3Base):
    argument_types = [
        ArgTypeSequence([DataType.DATE, DataType.CONST_STRING, DataType.INTEGER]),
    ]


class FuncDateadd3DatetimeConstNum(FuncDateadd3Base):
    argument_types = [
        ArgTypeSequence([_DATETIME_TYPES, DataType.CONST_STRING, DataType.CONST_INTEGER]),
    ]


class FuncDateadd3DatetimeNonConstNum(FuncDateadd3Base):
    argument_types = [
        ArgTypeSequence([DataType.DATETIME, DataType.CONST_STRING, DataType.INTEGER]),
    ]


class FuncDateadd3GenericDatetimeNonConstNum(FuncDateadd3Base):
    argument_types = [
        ArgTypeSequence([DataType.GENERICDATETIME, DataType.CONST_STRING, DataType.INTEGER]),
    ]


class FuncDateadd3DatetimeTZNonConstNum(FuncDateadd3Base):
    argument_types = [
        ArgTypeSequence([DataType.DATETIMETZ, DataType.CONST_STRING, DataType.INTEGER]),
    ]


class FuncDatepart(DateFunction):
    name = "datepart"
    arg_names = ["datetime", "unit", "firstday"]
    return_type = Fixed(DataType.INTEGER)


class FuncDatepart2Legacy(FuncDatepart):
    """Legacy version of the function with `date` as the last argument"""

    arg_cnt = 2
    variants = [
        VW(D.DUMMY | D.SQLITE, lambda what, date: n.func.DATEPART(date, what)),
    ]
    argument_types = [
        ArgTypeSequence([DataType.STRING, DataType.DATE]),
        ArgTypeSequence([DataType.STRING, DataType.DATETIME]),
        ArgTypeSequence([DataType.STRING, DataType.GENERICDATETIME]),
    ]
    return_flags = ContextFlag.DEPRECATED


class FuncDatepart2(FuncDatepart):
    arg_cnt = 2
    variants = [
        VW(D.DUMMY | D.SQLITE, lambda date, part: n.func.DATEPART(date, part, "mon")),
    ]
    argument_types = [
        ArgTypeSequence(
            [
                {DataType.DATE, DataType.DATETIME, DataType.GENERICDATETIME, DataType.DATETIMETZ},
                DataType.STRING,
            ]
        ),
    ]


@ensure_naive_first_arg
def _datepart_const_impl(date_ctx, part_ctx, firstday_ctx):
    """`DATEPART` for `(date|datetime, str, str)`"""
    part: str = un_literal(part_ctx.expression)

    if part == "second":
        return n.func.SECOND(date_ctx)
    if part == "minute":
        return n.func.MINUTE(date_ctx)
    if part == "hour":
        return n.func.HOUR(date_ctx)
    if part == "day":
        return n.func.DAY(date_ctx)
    if part == "month":
        return n.func.MONTH(date_ctx)
    if part == "quarter":
        return n.func.QUARTER(date_ctx)
    if part == "year":
        return n.func.YEAR(date_ctx)
    if part in ("dow", "dayofweek"):
        firstday: str = un_literal(firstday_ctx.expression)
        return n.func.DAYOFWEEK(date_ctx, firstday)
    if part == "week":
        return n.func.WEEK(date_ctx)

    # TODO?: raise an exception to user?
    return n.null()


class FuncDatepart3Const(FuncDatepart):
    arg_cnt = 3
    variants = [
        VW(D.DUMMY | D.SQLITE, _datepart_const_impl),
    ]
    argument_types = [
        ArgTypeSequence(
            [
                {DataType.DATE, DataType.DATETIME, DataType.GENERICDATETIME, DataType.DATETIMETZ},
                DataType.CONST_STRING,
                DataType.CONST_STRING,
            ]
        ),
    ]


@ensure_naive_first_arg
def _datepart_nonconst_impl(date_ctx, part_ctx, firstday_ctx):
    firstday: str = un_literal(firstday_ctx.expression)
    return (
        n.case(part_ctx)
        .whens(
            n.when("second").then(n.func.SECOND(date_ctx)),
            n.when("minute").then(n.func.MINUTE(date_ctx)),
            n.when("hour").then(n.func.HOUR(date_ctx)),
            n.when("day").then(n.func.DAY(date_ctx)),
            n.when("month").then(n.func.MONTH(date_ctx)),
            n.when("quarter").then(n.func.QUARTER(date_ctx)),
            n.when("year").then(n.func.YEAR(date_ctx)),
            n.when("dow").then(n.func.DAYOFWEEK(date_ctx, firstday)),
            n.when("dayofweek").then(n.func.DAYOFWEEK(date_ctx, firstday)),
            n.when("week").then(n.func.WEEK(date_ctx)),
            n.when("second").then(n.func.SECOND(date_ctx)),
        )
        .else_(None)
    )


class FuncDatepart3NonConst(FuncDatepart):
    arg_cnt = 3
    variants = [
        VW(D.DUMMY | D.SQLITE, _datepart_nonconst_impl),
    ]
    argument_types = [
        ArgTypeSequence(
            [
                {DataType.DATE, DataType.DATETIME, DataType.GENERICDATETIME, DataType.DATETIMETZ},
                DataType.STRING,
                DataType.CONST_STRING,
            ]
        ),
    ]


def norm_datetrunc_unit(unit):
    return normalize_and_validate_datetime_interval_type(un_literal(unit))


class FuncDatetrunc(DateFunction):
    name = "datetrunc"
    arg_names = ["datetime", "unit", "number"]
    return_type = FromArgs(0)
    return_type_params = ParamsFromArgs(0)


class FuncDatetrunc2(FuncDatetrunc):
    arg_cnt = 2


class FuncDatetrunc2Date(FuncDatetrunc2):
    argument_types = [
        ArgTypeSequence([DataType.DATE, DataType.CONST_STRING]),
    ]


class FuncDatetrunc2Datetime(FuncDatetrunc2):
    argument_types = [
        ArgTypeSequence([{DataType.DATETIME, DataType.GENERICDATETIME}, DataType.CONST_STRING]),
    ]


class FuncDatetrunc2DatetimeTZ(FuncDatetrunc2Datetime):
    argument_types = [ArgTypeSequence([DataType.DATETIMETZ, DataType.CONST_STRING])]


class FuncDatetrunc3(FuncDatetrunc):
    arg_cnt = 3


class FuncDatetrunc3Date(FuncDatetrunc3):
    argument_types = [
        ArgTypeSequence([DataType.DATE, DataType.CONST_STRING, DataType.CONST_INTEGER]),
    ]


class FuncDatetrunc3Datetime(FuncDatetrunc3):
    argument_types = [
        ArgTypeSequence(
            [
                {DataType.DATETIME, DataType.GENERICDATETIME, DataType.DATETIMETZ},
                DataType.CONST_STRING,
                DataType.CONST_INTEGER,
            ]
        ),
    ]


class SpecificDatepartFunc(DateFunction):
    arg_cnt = 1
    _zero_variants = [V(D.DUMMY | D.SQLITE, lambda x: sa.literal(0))]
    return_type = Fixed(DataType.INTEGER)


class FuncSecond(SpecificDatepartFunc):
    name = "second"
    arg_names = ["datetime"]


class FuncSecondDate(FuncSecond):
    argument_types = [
        ArgTypeSequence([DataType.DATE]),
    ]
    variants = SpecificDatepartFunc._zero_variants


class FuncSecondDatetime(FuncSecond):
    arg_cnt = 1
    argument_types = [ArgTypeSequence([{DataType.DATETIME, DataType.GENERICDATETIME}])]
    return_type = Fixed(DataType.INTEGER)


class FuncSecondDatetimeTZ(FuncSecondDatetime):
    argument_types = [ArgTypeSequence([DataType.DATETIMETZ])]
    return_type = Fixed(DataType.INTEGER)


class FuncMinute(SpecificDatepartFunc):
    name = "minute"
    arg_names = ["datetime"]


class FuncMinuteDate(FuncMinute):
    argument_types = [
        ArgTypeSequence([DataType.DATE]),
    ]
    variants = SpecificDatepartFunc._zero_variants


class FuncMinuteDatetime(FuncMinute):
    arg_cnt = 1
    argument_types = [ArgTypeSequence([{DataType.DATETIME, DataType.GENERICDATETIME}])]
    return_type = Fixed(DataType.INTEGER)


class FuncMinuteDatetimeTZ(FuncMinuteDatetime):
    argument_types = [ArgTypeSequence([DataType.DATETIMETZ])]
    return_type = Fixed(DataType.INTEGER)


class FuncHour(SpecificDatepartFunc):
    name = "hour"
    arg_names = ["datetime"]


class FuncHourDate(FuncHour):
    argument_types = [
        ArgTypeSequence([DataType.DATE]),
    ]
    variants = SpecificDatepartFunc._zero_variants


class FuncHourDatetime(FuncHour):
    arg_cnt = 1
    argument_types = [ArgTypeSequence([{DataType.DATETIME, DataType.GENERICDATETIME}])]
    return_type = Fixed(DataType.INTEGER)


class FuncHourDatetimeTZ(FuncHourDatetime):
    argument_types = [ArgTypeSequence([DataType.DATETIMETZ])]
    return_type = Fixed(DataType.INTEGER)


class FuncDay(SpecificDatepartFunc):
    name = "day"
    arg_cnt = 1
    arg_names = ["datetime"]
    argument_types = [ArgTypeSequence([{DataType.DATE, DataType.DATETIME, DataType.GENERICDATETIME}])]
    return_type = Fixed(DataType.INTEGER)


class FuncDayDatetimeTZ(FuncDay):
    argument_types = [ArgTypeSequence([DataType.DATETIMETZ])]
    return_type = Fixed(DataType.INTEGER)


class FuncMonth(SpecificDatepartFunc):
    name = "month"
    arg_names = ["datetime"]
    arg_cnt = 1
    argument_types = [ArgTypeSequence([{DataType.DATE, DataType.DATETIME, DataType.GENERICDATETIME}])]
    return_type = Fixed(DataType.INTEGER)


class FuncMonthDatetimeTZ(FuncMonth):
    argument_types = [ArgTypeSequence([DataType.DATETIMETZ])]
    return_type = Fixed(DataType.INTEGER)


class FuncQuarter(SpecificDatepartFunc):
    name = "quarter"
    arg_names = ["datetime"]
    arg_cnt = 1
    argument_types = [ArgTypeSequence([{DataType.DATE, DataType.DATETIME, DataType.GENERICDATETIME}])]
    return_type = Fixed(DataType.INTEGER)


class FuncQuarterDatetimeTZ(FuncQuarter):
    argument_types = [ArgTypeSequence([DataType.DATETIMETZ])]
    return_type = Fixed(DataType.INTEGER)


class FuncYear(SpecificDatepartFunc):
    name = "year"
    arg_names = ["datetime"]
    arg_cnt = 1
    argument_types = [ArgTypeSequence([{DataType.DATE, DataType.DATETIME, DataType.GENERICDATETIME}])]
    return_type = Fixed(DataType.INTEGER)


class FuncYearDatetimeTZ(FuncYear):
    argument_types = [ArgTypeSequence([DataType.DATETIMETZ])]
    return_type = Fixed(DataType.INTEGER)


class FuncDayofweek(SpecificDatepartFunc):
    name = "dayofweek"
    arg_names = ["datetime", "firstday"]
    return_type = Fixed(DataType.INTEGER)

    _fd_map = {
        "mon": 1,
        "monday": 1,
        "tue": 2,
        "tuesday": 2,
        "wed": 3,
        "wednesday": 3,
        "thu": 4,
        "thursday": 4,
        "fri": 5,
        "friday": 5,
        "sat": 6,
        "saturday": 6,
        "sun": 7,
        "sunday": 7,
    }

    @classmethod
    def _norm_fd(cls, firstday):
        """Normalize first-day-of-the-week value (convert to int (1-7))"""
        try:
            return cls._fd_map[un_literal(firstday).lower()]
        except KeyError:
            raise exc.TranslationError("Unknown time unit")


norm_fd = FuncDayofweek._norm_fd


class FuncDayofweek1(FuncDayofweek):
    arg_cnt = 1
    variants = [
        VW(D.DUMMY | D.SQLITE, lambda date: n.func.DAYOFWEEK(date.expression, "mon")),
    ]
    argument_types = [
        ArgTypeSequence([{DataType.DATE, DataType.DATETIME, DataType.GENERICDATETIME, DataType.DATETIMETZ}]),
    ]


def dow_firstday_shift(base_1_dow, firstday_expr):
    """
    `base_1_dow` should be `1` for monday, ..., `6` for saturday, `7` for sunday
    """
    firstday = norm_fd(firstday_expr)
    if firstday == 1:
        return base_1_dow
    return (base_1_dow - firstday + 7) % 7 + 1


class FuncDayofweek2(FuncDayofweek):
    arg_cnt = 2
    argument_types = [
        ArgTypeSequence([{DataType.DATE, DataType.DATETIME, DataType.GENERICDATETIME}, DataType.CONST_STRING]),
    ]


class FuncDayofweek2TZ(FuncDayofweek):
    arg_cnt = 2
    argument_types = [
        ArgTypeSequence([DataType.DATETIMETZ, DataType.CONST_STRING]),
    ]


class FuncWeek(SpecificDatepartFunc):
    name = "week"
    arg_cnt = 1
    argument_types = [
        ArgTypeSequence([{DataType.DATE, DataType.DATETIME, DataType.GENERICDATETIME}]),
    ]
    return_type = Fixed(DataType.INTEGER)


class FuncWeekTZ(FuncWeek):
    argument_types = [ArgTypeSequence([DataType.DATETIMETZ])]


class FuncTypeGenericDatetimeNowImpl(DateFunction):
    arg_cnt = 0
    return_type = Fixed(DataType.GENERICDATETIME)


class FuncNow(FuncTypeGenericDatetimeNowImpl):
    name = "now"


class FuncGenericNow(FuncTypeGenericDatetimeNowImpl):
    name = "genericnow"
    scopes = Function.scopes & ~Scope.SUGGESTED & ~Scope.DOCUMENTED


class FuncToday(DateFunction):
    name = "today"
    arg_cnt = 0
    return_type = Fixed(DataType.DATE)


# TODO: UTCTODAY() through DataType.DATETIMETZ / DATE(UTCNOW()), and remove the 'UTC' cast from TODAY().


DEFINITIONS_DATETIME = [
    # dateadd
    FuncDateadd1,
    FuncDateadd2Unit,
    FuncDateadd2Number,
    FuncDateadd3Legacy,
    FuncDateadd3DateConstNum,
    FuncDateadd3DateNonConstNum,
    FuncDateadd3DatetimeConstNum,
    FuncDateadd3DatetimeNonConstNum,
    FuncDateadd3GenericDatetimeNonConstNum,
    FuncDateadd3DatetimeTZNonConstNum,
    # datepart
    FuncDatepart2Legacy,
    FuncDatepart2,
    FuncDatepart3Const,
    FuncDatepart3NonConst,
    # datetrunc
    FuncDatetrunc2Date,
    FuncDatetrunc2Datetime,
    FuncDatetrunc2DatetimeTZ,
    FuncDatetrunc3Date,
    FuncDatetrunc3Datetime,
    # day
    FuncDay,
    FuncDayDatetimeTZ,
    # dayofweek
    FuncDayofweek1,
    FuncDayofweek2,
    FuncDayofweek2TZ,
    # genericnow
    FuncGenericNow,
    # hour
    FuncHourDate,
    FuncHourDatetime,
    FuncHourDatetimeTZ,
    # minute
    FuncMinuteDate,
    FuncMinuteDatetime,
    FuncMinuteDatetimeTZ,
    # month
    FuncMonth,
    FuncMonthDatetimeTZ,
    # now
    FuncNow,
    # quarter
    FuncQuarter,
    FuncQuarterDatetimeTZ,
    # second
    FuncSecondDate,
    FuncSecondDatetime,
    FuncSecondDatetimeTZ,
    # today
    FuncToday,
    # week
    FuncWeek,
    FuncWeekTZ,
    # year
    FuncYear,
    FuncYearDatetimeTZ,
]
