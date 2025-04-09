
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
    # base.FuncDatepart2.for_dialect(D.TRINO),
    # base.FuncDatepart3Const.for_dialect(D.TRINO),
    # base.FuncDatepart3NonConst.for_dialect(D.TRINO),
    # datetrunc
    # base.FuncDatetrunc2Date.for_dialect(D.TRINO),
    # base.FuncDatetrunc2Datetime.for_dialect(D.TRINO),
    # base.FuncDatetrunc2DatetimeTZ.for_dialect(D.TRINO),
    # base.FuncDatetrunc3Date.for_dialect(D.TRINO),
    # base.FuncDatetrunc3Datetime.for_dialect(D.TRINO),
    # day
    # base.FuncDay.for_dialect(D.TRINO),
    # base.FuncDayDatetimeTZ.for_dialect(D.TRINO),
    # dayofweek
    # base.FuncDayofweek1.for_dialect(D.TRINO),
    # base.FuncDayofweek2.for_dialect(D.TRINO),
    # base.FuncDayofweek2TZ.for_dialect(D.TRINO),
    # genericnow
    base.FuncGenericNow(
        variants=[
            V(D.TRINO, sa.func.current_timestamp),
        ]
    ),
    # hour
    # base.FuncHourDate.for_dialect(D.TRINO),
    # base.FuncHourDatetime.for_dialect(D.TRINO),
    # base.FuncHourDatetimeTZ.for_dialect(D.TRINO),
    # minute
    # base.FuncMinuteDate.for_dialect(D.TRINO),
    # base.FuncMinuteDatetime.for_dialect(D.TRINO),
    # base.FuncMinuteDatetimeTZ.for_dialect(D.TRINO),
    # month
    # base.FuncMonth.for_dialect(D.TRINO),
    # base.FuncMonthDatetimeTZ.for_dialect(D.TRINO),
    # now
    base.FuncNow(
        variants=[
            V(D.TRINO, sa.func.current_timestamp),
        ]
    ),
    # quarter
    # base.FuncQuarter.for_dialect(D.TRINO),
    # base.FuncQuarterDatetimeTZ.for_dialect(D.TRINO),
    # second
    # base.FuncSecondDate.for_dialect(D.TRINO),
    # base.FuncSecondDatetime.for_dialect(D.TRINO),
    # base.FuncSecondDatetimeTZ.for_dialect(D.TRINO),
    # today
    base.FuncToday(
        variants=[
            V(D.TRINO, sa.func.current_date),
        ]
    ),
    # week
    # base.FuncWeek.for_dialect(D.TRINO),
    # base.FuncWeekTZ.for_dialect(D.TRINO),
    # year
    # base.FuncYear.for_dialect(D.TRINO),
    # base.FuncYearDatetimeTZ.for_dialect(D.TRINO),
]
