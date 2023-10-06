import sqlalchemy as sa

from dl_formula.definitions.base import TranslationVariant
from dl_formula.definitions.common import raw_sql
import dl_formula.definitions.functions_datetime as base

from dl_connector_snowflake.formula.constants import SnowFlakeDialect as D


V = TranslationVariant.make


DEFINITIONS_DATETIME = [
    # dateadd
    base.FuncDateadd3Legacy.for_dialect(D.SNOWFLAKE),
    base.FuncDateadd1.for_dialect(D.SNOWFLAKE),
    base.FuncDateadd2Unit.for_dialect(D.SNOWFLAKE),
    base.FuncDateadd2Number.for_dialect(D.SNOWFLAKE),
    base.FuncDateadd3DateConstNum(variants=[V(D.SNOWFLAKE, lambda date, what, num: sa.func.DATEADD(what, num, date))]),
    base.FuncDateadd3DatetimeConstNum(
        variants=[V(D.SNOWFLAKE, lambda date, what, num: sa.func.DATEADD(what, num, date))]
    ),
    # datepart
    base.FuncDatepart2.for_dialect(D.SNOWFLAKE),
    base.FuncDatepart3Const.for_dialect(D.SNOWFLAKE),
    base.FuncDatepart3NonConst.for_dialect(D.SNOWFLAKE),
    # datetrunc
    base.FuncDatetrunc2Date(
        variants=[
            V(
                D.SNOWFLAKE,
                lambda date, unit: (
                    sa.func.DATE_TRUNC(
                        raw_sql(base.norm_datetrunc_unit(unit).replace("week", "week")),
                        date,
                    )
                    if base.norm_datetrunc_unit(unit) in {"year", "quarter", "month", "week", "day"}
                    else date
                ),
            ),
        ]
    ),
    base.FuncDatetrunc2Datetime(
        variants=[
            V(
                D.SNOWFLAKE,
                lambda date, unit: sa.func.DATE_TRUNC(
                    raw_sql(base.norm_datetrunc_unit(unit).replace("week", "week")),
                    date,
                ),
            ),
        ]
    ),
    # day
    base.FuncDay(
        variants=[
            V(D.SNOWFLAKE, lambda value: sa.func.EXTRACT("DAY", value)),
        ]
    ),
    # dayofweek
    base.FuncDayofweek1(
        variants=[
            V(
                D.SNOWFLAKE,
                sa.func.DAYOFWEEKISO,
            )
        ]
    ),
    base.FuncDayofweek2(
        variants=[
            V(
                D.SNOWFLAKE,
                lambda date, firstday: sa.func.MOD(sa.func.DAYOFWEEKISO(date) - base.norm_fd(firstday), 7) + 1,
            ),
        ]
    ),
    # genericnow
    base.FuncGenericNow(
        variants=[
            V(D.SNOWFLAKE, lambda: sa.func.CONVERT_TIMEZONE("UTC", sa.func.LOCALTIMESTAMP())),
        ]
    ),
    # hour
    base.FuncHourDate.for_dialect(D.SNOWFLAKE),
    base.FuncHourDatetime(
        variants=[
            V(D.SNOWFLAKE, lambda value: sa.func.EXTRACT("HOUR", value)),
        ]
    ),
    # minute
    base.FuncMinuteDate.for_dialect(D.SNOWFLAKE),
    base.FuncMinuteDatetime(
        variants=[
            V(D.SNOWFLAKE, lambda value: sa.func.EXTRACT("MINUTE", value)),
        ]
    ),
    # month
    base.FuncMonth(
        variants=[
            V(D.SNOWFLAKE, lambda value: sa.func.EXTRACT("MONTH", value)),
        ]
    ),
    # now
    base.FuncNow(
        variants=[
            V(D.SNOWFLAKE, lambda: sa.func.CONVERT_TIMEZONE("UTC", sa.func.LOCALTIMESTAMP())),
        ]
    ),
    # quarter
    base.FuncQuarter(
        variants=[
            V(D.SNOWFLAKE, lambda value: sa.func.EXTRACT("QUARTER", value)),
        ]
    ),
    # second
    base.FuncSecondDate.for_dialect(D.SNOWFLAKE),
    base.FuncSecondDatetime(
        variants=[
            V(D.SNOWFLAKE, lambda value: sa.func.EXTRACT("SECOND", value)),
        ]
    ),
    # today
    base.FuncToday(
        variants=[
            V(D.SNOWFLAKE, sa.func.CURRENT_DATE),
        ]
    ),
    # week
    base.FuncWeek(
        variants=[
            V(D.SNOWFLAKE, sa.func.WEEKISO),
        ]
    ),
    # year
    base.FuncYear(
        variants=[
            V(D.SNOWFLAKE, lambda value: sa.func.EXTRACT("YEAR", value)),
        ]
    ),
]
