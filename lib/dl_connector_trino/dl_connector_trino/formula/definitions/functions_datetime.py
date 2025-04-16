import sqlalchemy as sa

from dl_formula.definitions.base import (
    TranslationVariant,
    TranslationVariantWrapped,
)
from dl_formula.definitions.common_datetime import ensure_naive_first_arg
import dl_formula.definitions.functions_datetime as base
from dl_formula.shortcuts import n

from dl_connector_trino.formula.constants import TrinoDialect as D


V = TranslationVariant.make
VW = TranslationVariantWrapped.make

DEFINITIONS_DATETIME = [
    # dateadd
    base.FuncDateadd1.for_dialect(D.TRINO),
    base.FuncDateadd2Unit.for_dialect(D.TRINO),
    base.FuncDateadd2Number.for_dialect(D.TRINO),
    base.FuncDateadd3Legacy.for_dialect(D.TRINO),
    base.FuncDateadd3DateNonConstNum(
        variants=[
            V(D.TRINO, lambda timestamp, unit, num: sa.func.date_add(unit, num, timestamp)),
        ]
    ),
    # base.FuncDateadd3DatetimeNonConstNum.for_dialect(D.TRINO),
    base.FuncDateadd3GenericDatetimeNonConstNum(
        variants=[
            V(D.TRINO, lambda timestamp, unit, num: sa.func.date_add(unit, num, timestamp)),
        ]
    ),
    base.FuncDateadd3DatetimeTZNonConstNum(
        variants=[
            V(D.TRINO, lambda timestamp, unit, num: sa.func.date_add(unit, num, timestamp)),
        ]
    ),
    # datepart
    base.FuncDatepart2Legacy.for_dialect(D.TRINO),
    base.FuncDatepart2.for_dialect(D.TRINO),
    # base.FuncDatepart3Const.for_dialect(D.TRINO),
    base.FuncDatepart3NonConst.for_dialect(D.TRINO),
    # datetrunc
    base.FuncDatetrunc2Date(
        variants=[
            V(
                D.TRINO,
                lambda date, unit: date
                if base.norm_datetrunc_unit(unit) in ("second", "minute", "hour", "day")
                else sa.func.date_trunc(unit, date),
            ),
        ]
    ),
    base.FuncDatetrunc2Datetime(
        variants=[
            V(
                D.TRINO,
                lambda datetime, unit: sa.func.date_trunc(base.norm_datetrunc_unit(unit), datetime),
            ),
        ]
    ),
    base.FuncDatetrunc2DatetimeTZ(
        variants=[
            V(
                D.TRINO,
                lambda datetime, unit: sa.func.date_trunc(base.norm_datetrunc_unit(unit), datetime),
            ),
        ]
    ),
    # base.FuncDatetrunc3Date.for_dialect(D.TRINO),
    # base.FuncDatetrunc3Datetime.for_dialect(D.TRINO),
    # day
    base.FuncDay(
        variants=[
            V(D.TRINO, sa.func.day),
        ]
    ),
    base.FuncDayDatetimeTZ(
        variants=[
            VW(D.TRINO, ensure_naive_first_arg(n.func.DAY)),
        ]
    ),
    # dayofweek
    base.FuncDayofweek1(
        variants=[
            V(D.TRINO, sa.func.day_of_week),
        ]
    ),
    base.FuncDayofweek2(
        variants=[
            V(D.TRINO, lambda date, firstday: base.dow_firstday_shift(sa.func.day_of_week(date), firstday)),
        ]
    ),
    base.FuncDayofweek2TZ(
        variants=[
            V(D.TRINO, lambda date, firstday: base.dow_firstday_shift(sa.func.day_of_week(date), firstday)),
        ]
    ),
    # genericnow
    base.FuncGenericNow(
        variants=[
            V(D.TRINO, sa.func.current_timestamp),
        ]
    ),
    # hour
    base.FuncHourDate(
        variants=[
            V(D.TRINO, sa.func.hour),
        ]
    ),
    base.FuncHourDatetime(
        variants=[
            V(D.TRINO, sa.func.hour),
        ]
    ),
    base.FuncHourDatetimeTZ(
        variants=[
            VW(D.TRINO, ensure_naive_first_arg(n.func.HOUR)),
        ]
    ),
    # minute
    base.FuncMinuteDate(
        variants=[
            V(D.TRINO, sa.func.minute),
        ]
    ),
    base.FuncMinuteDatetime(
        variants=[
            V(D.TRINO, sa.func.minute),
        ]
    ),
    base.FuncMinuteDatetimeTZ(
        variants=[
            VW(D.TRINO, ensure_naive_first_arg(n.func.MINUTE)),
        ]
    ),
    # month
    base.FuncMonth(
        variants=[
            V(D.TRINO, sa.func.month),
        ]
    ),
    base.FuncMonthDatetimeTZ(
        variants=[
            VW(D.TRINO, ensure_naive_first_arg(n.func.MONTH)),
        ]
    ),
    # now
    base.FuncNow(
        variants=[
            V(D.TRINO, sa.func.current_timestamp),
        ]
    ),
    # quarter
    base.FuncQuarter(
        variants=[
            V(D.TRINO, sa.func.quarter),
        ]
    ),
    base.FuncQuarterDatetimeTZ(
        variants=[
            VW(D.TRINO, ensure_naive_first_arg(n.func.QUARTER)),
        ]
    ),
    # second
    base.FuncSecondDate(
        variants=[
            V(D.TRINO, sa.func.second),
        ]
    ),
    base.FuncSecondDatetime(
        variants=[
            V(D.TRINO, sa.func.second),
        ]
    ),
    base.FuncSecondDatetimeTZ(
        variants=[
            VW(D.TRINO, ensure_naive_first_arg(n.func.SECOND)),
        ]
    ),
    # today
    base.FuncToday(
        variants=[
            V(D.TRINO, sa.func.current_date),
        ]
    ),
    # week
    base.FuncWeek(
        variants=[
            V(D.TRINO, sa.func.week),
        ]
    ),
    base.FuncWeekTZ(
        variants=[
            VW(D.TRINO, ensure_naive_first_arg(n.func.WEEK)),
        ]
    ),
    # year
    base.FuncYear(
        variants=[
            V(D.TRINO, sa.func.year),
        ]
    ),
    base.FuncYearDatetimeTZ(
        variants=[
            VW(D.TRINO, ensure_naive_first_arg(n.func.YEAR)),
        ]
    ),
]
