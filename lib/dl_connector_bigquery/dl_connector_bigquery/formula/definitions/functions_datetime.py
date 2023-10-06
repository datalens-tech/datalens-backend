import sqlalchemy as sa
from sqlalchemy.sql.elements import ClauseElement

import dl_formula.core.exc as exc
from dl_formula.definitions.base import TranslationVariant
from dl_formula.definitions.common import raw_sql
from dl_formula.definitions.common_datetime import datetime_interval
import dl_formula.definitions.functions_datetime as base
from dl_formula.definitions.literals import un_literal

from dl_connector_bigquery.formula.constants import BigQueryDialect as D


V = TranslationVariant.make


def _bq_dynamic_interval(what: str, num: int) -> ClauseElement:
    what = what.lower()
    parts = [0, 0, 0, 0, 0, 0]
    part_names = ["year", "month", "day", "hour", "minute", "second"]
    if what not in part_names:
        raise exc.TranslationError(f"Invalid interval name: {what}")
    parts[part_names.index(what)] = num
    return sa.func.MAKE_INTERVAL(*parts)


DEFINITIONS_DATETIME = [
    # dateadd
    base.FuncDateadd1.for_dialect(D.BIGQUERY),
    base.FuncDateadd2Unit.for_dialect(D.BIGQUERY),
    base.FuncDateadd2Number.for_dialect(D.BIGQUERY),
    base.FuncDateadd3DateConstNum(
        variants=[
            V(
                D.BIGQUERY,
                lambda date, what, num: sa.func.DATE_ADD(
                    date, datetime_interval(un_literal(what), mult=un_literal(num))
                ),
            ),
        ]
    ),
    base.FuncDateadd3DateNonConstNum(
        variants=[
            V(D.BIGQUERY, lambda date, what, num: sa.func.DATE_ADD(date, _bq_dynamic_interval(un_literal(what), num))),
        ]
    ),
    base.FuncDateadd3DatetimeConstNum(
        variants=[
            V(
                D.BIGQUERY,
                lambda date, what, num: sa.func.DATE_ADD(
                    date, datetime_interval(un_literal(what), mult=un_literal(num))
                ),
            ),
        ]
    ),
    base.FuncDateadd3DatetimeNonConstNum(
        variants=[
            V(D.BIGQUERY, lambda date, what, num: sa.func.DATE_ADD(date, _bq_dynamic_interval(un_literal(what), num))),
        ]
    ),
    base.FuncDateadd3GenericDatetimeNonConstNum(
        variants=[
            V(D.BIGQUERY, lambda date, what, num: sa.func.DATE_ADD(date, _bq_dynamic_interval(un_literal(what), num))),
        ]
    ),
    # datepart
    base.FuncDatepart2.for_dialect(D.BIGQUERY),
    base.FuncDatepart3Const.for_dialect(D.BIGQUERY),
    base.FuncDatepart3NonConst.for_dialect(D.BIGQUERY),
    # datetrunc
    base.FuncDatetrunc2Date(
        variants=[
            V(
                D.BIGQUERY,
                lambda date, unit: (
                    sa.func.DATE_TRUNC(date, raw_sql(base.norm_datetrunc_unit(unit).replace("week", "week(monday)")))
                    if base.norm_datetrunc_unit(unit) in {"year", "quarter", "month", "week", "day"}
                    else date
                ),
            ),
        ]
    ),
    base.FuncDatetrunc2Datetime(
        variants=[
            V(
                D.BIGQUERY,
                lambda date, unit: sa.func.DATETIME_TRUNC(
                    date, raw_sql(base.norm_datetrunc_unit(unit).replace("week", "week(monday)"))
                ),
            ),
        ]
    ),
    # day
    base.FuncDay(
        variants=[
            V(D.BIGQUERY, lambda value: sa.func.EXTRACT("DAY", value)),
        ]
    ),
    # dayofweek
    base.FuncDayofweek1.for_dialect(D.BIGQUERY),
    base.FuncDayofweek2(
        variants=[
            V(
                D.BIGQUERY,
                lambda date, firstday: sa.func.MOD(sa.func.EXTRACT("DAYOFWEEK", date) - 1 - base.norm_fd(firstday), 7)
                + 1,
            ),
        ]
    ),
    # genericnow
    base.FuncGenericNow(
        variants=[
            V(D.BIGQUERY, sa.func.CURRENT_DATETIME),
        ]
    ),
    # hour
    base.FuncHourDate.for_dialect(D.BIGQUERY),
    base.FuncHourDatetime(
        variants=[
            V(D.BIGQUERY, lambda value: sa.func.EXTRACT("HOUR", value)),
        ]
    ),
    # minute
    base.FuncMinuteDate.for_dialect(D.BIGQUERY),
    base.FuncMinuteDatetime(
        variants=[
            V(D.BIGQUERY, lambda value: sa.func.EXTRACT("MINUTE", value)),
        ]
    ),
    # month
    base.FuncMonth(
        variants=[
            V(D.BIGQUERY, lambda value: sa.func.EXTRACT("MONTH", value)),
        ]
    ),
    # now
    base.FuncNow(
        variants=[
            V(D.BIGQUERY, sa.func.CURRENT_DATETIME),
        ]
    ),
    # quarter
    base.FuncQuarter(
        variants=[
            V(D.BIGQUERY, lambda value: sa.func.EXTRACT("QUARTER", value)),
        ]
    ),
    # second
    base.FuncSecondDate.for_dialect(D.BIGQUERY),
    base.FuncSecondDatetime(
        variants=[
            V(D.BIGQUERY, lambda value: sa.func.EXTRACT("SECOND", value)),
        ]
    ),
    # today
    base.FuncToday(
        variants=[
            V(D.BIGQUERY, sa.func.CURRENT_DATE),
        ]
    ),
    # week
    base.FuncWeek(
        variants=[
            V(D.BIGQUERY, lambda value: sa.func.EXTRACT("ISOWEEK", value)),
        ]
    ),
    # year
    base.FuncYear(
        variants=[
            V(D.BIGQUERY, lambda value: sa.func.EXTRACT("YEAR", value)),
        ]
    ),
]
