
from dl_formula.definitions.base import (
    TranslationVariant,
    TranslationVariantWrapped,
)

# from sqlalchemy.sql.elements import ClauseElement



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
    # base.FuncDateadd3DatetimeTZNonConstNum.for_dialect(D.TRINO),
    # datepart
    # base.FuncDatepart2Legacy.for_dialect(D.TRINO),
    base.FuncDatepart2(
        variants=[
            V(
                D.TRINO,
                lambda timestamp, unit: sa.case(
                    [
                        (unit == "second", sa.func.second(timestamp)),
                        (unit == "minute", sa.func.minute(timestamp)),
                        (unit == "hour", sa.func.hour(timestamp)),
                        (unit == "day", sa.func.day(timestamp)),
                        (unit == "week", sa.func.week(timestamp)),
                        (unit == "month", sa.func.month(timestamp)),
                        (unit == "quarter", sa.func.quarter(timestamp)),
                        (unit == "year", sa.func.year(timestamp)),
                        ((unit == "dayofweek") | (unit == "dow"), sa.func.day_of_week(timestamp)),
                    ],
                ),
            ),
        ]
    ),
    # base.FuncDatepart3Const.for_dialect(D.TRINO),
    # base.FuncDatepart3NonConst.for_dialect(D.TRINO),
    # datetrunc
    # base.FuncDatetrunc2Date.for_dialect(D.TRINO),
    # base.FuncDatetrunc2Datetime.for_dialect(D.TRINO),
    # base.FuncDatetrunc2DatetimeTZ.for_dialect(D.TRINO),
    # base.FuncDatetrunc3Date.for_dialect(D.TRINO),
    # base.FuncDatetrunc3Datetime.for_dialect(D.TRINO),
    # day
    base.FuncDay(
        variants=[
            V(D.TRINO, sa.func.day),
        ]
    ),
    # base.FuncDayDatetimeTZ.for_dialect(D.TRINO),
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
    # base.FuncDayofweek2TZ.for_dialect(D.TRINO),
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
    # base.FuncHourDatetimeTZ.for_dialect(D.TRINO),
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
    # base.FuncMinuteDatetimeTZ.for_dialect(D.TRINO),
    # month
    base.FuncMonth(
        variants=[
            V(D.TRINO, sa.func.month),
        ]
    ),
    # base.FuncMonthDatetimeTZ.for_dialect(D.TRINO),
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
    # base.FuncQuarterDatetimeTZ.for_dialect(D.TRINO),
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
    # base.FuncSecondDatetimeTZ.for_dialect(D.TRINO),
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
    # base.FuncWeekTZ.for_dialect(D.TRINO),
    # year
    base.FuncYear(
        variants=[
            V(D.TRINO, sa.func.year),
        ]
    ),
    # base.FuncYearDatetimeTZ.for_dialect(D.TRINO),
]
