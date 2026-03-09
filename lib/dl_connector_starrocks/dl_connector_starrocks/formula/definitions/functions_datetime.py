import sqlalchemy as sa

from dl_formula.definitions.base import TranslationVariant
from dl_formula.definitions.common_datetime import datetime_interval
import dl_formula.definitions.functions_datetime as base
from dl_formula.definitions.literals import un_literal

from dl_connector_starrocks.formula.constants import StarRocksDialect as D


V = TranslationVariant.make


DEFINITIONS_DATETIME = [
    # dateadd
    base.FuncDateadd1.for_dialect(D.STARROCKS),
    base.FuncDateadd2Unit.for_dialect(D.STARROCKS),
    base.FuncDateadd2Number.for_dialect(D.STARROCKS),
    base.FuncDateadd3Legacy.for_dialect(D.STARROCKS),
    base.FuncDateadd3DateConstNum(
        variants=[
            V(
                D.STARROCKS,
                lambda date, what, num: sa.type_coerce(
                    date + datetime_interval(un_literal(what), un_literal(num)), sa.Date
                ),
            ),
        ]
    ),
    base.FuncDateadd3DatetimeConstNum(
        variants=[
            V(D.STARROCKS, lambda dt, what, num: (dt + datetime_interval(what.value, num.value))),
        ]
    ),
    # datepart
    base.FuncDatepart2Legacy.for_dialect(D.STARROCKS),
    base.FuncDatepart2.for_dialect(D.STARROCKS),
    base.FuncDatepart3Const.for_dialect(D.STARROCKS),
    base.FuncDatepart3NonConst.for_dialect(D.STARROCKS),
    # datetrunc
    base.FuncDatetrunc2Date(
        variants=[
            V(
                D.STARROCKS,
                lambda date, unit: (
                    sa.cast(sa.func.date_trunc(base.norm_datetrunc_unit(unit), date), sa.Date)
                    if base.norm_datetrunc_unit(unit) in {"year", "quarter", "month", "week", "day"}
                    else date
                ),
            ),
        ]
    ),
    base.FuncDatetrunc2Datetime(
        variants=[
            V(
                D.STARROCKS,
                lambda date, unit: sa.func.date_trunc(base.norm_datetrunc_unit(unit), date),
            ),
        ],
    ),
    # day
    base.FuncDay(
        variants=[
            V(D.STARROCKS, sa.func.DAYOFMONTH),
        ]
    ),
    # dayofweek
    base.FuncDayofweek1.for_dialect(D.STARROCKS),
    base.FuncDayofweek2(
        variants=[
            V(
                D.STARROCKS,
                lambda date, firstday: base.dow_firstday_shift(sa.func.WEEKDAY(date) + 1, firstday),
            ),  # StarRocks WEEKDAY is 0 for monday, 1 for tuesday, ..., 6 for sunday (MySQL-compatible).
        ]
    ),
    # genericnow
    base.FuncGenericNow(
        variants=[
            V(D.STARROCKS, sa.func.NOW),
        ]
    ),
    # hour
    base.FuncHourDate.for_dialect(D.STARROCKS),
    base.FuncHourDatetime(
        variants=[
            V(D.STARROCKS, sa.func.HOUR),
        ]
    ),
    # minute
    base.FuncMinuteDate.for_dialect(D.STARROCKS),
    base.FuncMinuteDatetime(
        variants=[
            V(D.STARROCKS, sa.func.MINUTE),
        ]
    ),
    # month
    base.FuncMonth(
        variants=[
            V(D.STARROCKS, sa.func.MONTH),
        ]
    ),
    # now
    base.FuncNow(
        variants=[
            V(D.STARROCKS, sa.func.NOW),
        ]
    ),
    # quarter
    base.FuncQuarter(
        variants=[
            V(D.STARROCKS, sa.func.QUARTER),
        ]
    ),
    # second
    base.FuncSecondDate.for_dialect(D.STARROCKS),
    base.FuncSecondDatetime(
        variants=[
            V(D.STARROCKS, sa.func.SECOND),
        ]
    ),
    # today
    base.FuncToday(
        variants=[
            V(D.STARROCKS, sa.func.CURDATE),
        ]
    ),
    # week
    base.FuncWeek(
        variants=[
            V(D.STARROCKS, sa.func.WEEKOFYEAR),
        ]
    ),
    # year
    base.FuncYear(
        variants=[
            V(D.STARROCKS, sa.func.YEAR),
        ]
    ),
]
