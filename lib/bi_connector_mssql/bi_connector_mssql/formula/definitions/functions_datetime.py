from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.sql import ClauseElement

import dl_formula.definitions.functions_datetime as base
from dl_formula.definitions.base import TranslationVariant
from dl_formula.definitions.common import raw_sql
from dl_formula.definitions.literals import un_literal
from dl_formula.definitions.common_datetime import (
    EPOCH_START_S, EPOCH_START_DOW
)

from bi_connector_mssql.formula.constants import MssqlDialect as D


V = TranslationVariant.make


def make_mssql_datetrunc(date: ClauseElement, unit: str, start_of_year: ClauseElement) -> ClauseElement:
    norm_unit = base.norm_datetrunc_unit(unit)
    if norm_unit == 'week':
        return make_mssql_datetrunc_week(date)
    return start_of_year if norm_unit == 'year' else sa.func.DATEADD(
        raw_sql(norm_unit),
        sa.func.DATEDIFF(raw_sql(norm_unit), start_of_year, date),
        start_of_year,
    )


def make_mssql_datetrunc_week(date: ClauseElement) -> ClauseElement:
    set_day_to_monday = sa.func.DATEADD(
        sa.text('day'),
        # This monstrosity is used to get @@DATEFIRST independent day of week in MSSQL where Monday = 0
        # In order to understand that consider that for any values of @@DATEFIRST
        # (DATEPART(dw, :date) + @@DATEFIRST) % 7 returns the day of week number where
        # Saturday = 0, Sunday = 1, Monday = 2 and so on. So an offset of 5 shifts it to Monday = 0
        -((5 + sa.func.DATEPART(sa.text('dw'), date) + sa.text('@@DATEFIRST')) % 7),
        date
    )
    return sa.cast(
        sa.cast(set_day_to_monday, sa.types.DATE),
        sa.dialects.mssql.DATETIME
    )


DEFINITIONS_DATETIME = [
    # dateadd
    base.FuncDateadd1.for_dialect(D.MSSQLSRV),
    base.FuncDateadd2Unit.for_dialect(D.MSSQLSRV),
    base.FuncDateadd2Number.for_dialect(D.MSSQLSRV),
    base.FuncDateadd3Legacy.for_dialect(D.MSSQLSRV),
    base.FuncDateadd3DateConstNum(variants=[
        V(D.MSSQLSRV, lambda date, what, num: sa.cast(
            sa.func.DATEADD(raw_sql(un_literal(what)), un_literal(num), date), sa.Date)),
    ]),
    base.FuncDateadd3DatetimeConstNum(variants=[
        V(D.MSSQLSRV, lambda dt, what, num: (
            sa.cast(
                sa.func.DATEADD(raw_sql(what.value), num.value, dt),
                sa.DateTime))),
    ]),

    # datepart
    base.FuncDatepart2Legacy.for_dialect(D.MSSQLSRV),
    base.FuncDatepart2.for_dialect(D.MSSQLSRV),
    base.FuncDatepart3Const.for_dialect(D.MSSQLSRV),
    base.FuncDatepart3NonConst.for_dialect(D.MSSQLSRV),

    # datetrunc
    base.FuncDatetrunc2Date(variants=[
        V(D.MSSQLSRV, lambda date, unit: (
            make_mssql_datetrunc(
                date, unit, sa.func.DATEFROMPARTS(sa.func.YEAR(date), 1, 1)
            ) if base.norm_datetrunc_unit(unit) in {'year', 'quarter', 'month', 'week'} else
            date
        )),
    ]),
    base.FuncDatetrunc2Datetime(variants=[
        V(D.MSSQLSRV, lambda date, unit: make_mssql_datetrunc(
            date, unit, sa.func.DATETIMEFROMPARTS(sa.func.YEAR(date), 1, 1, 0, 0, 0, 0)
        )),
    ]),

    # day
    base.FuncDay(variants=[
        V(D.MSSQLSRV, sa.func.DAY),
    ]),

    # dayofweek
    base.FuncDayofweek1.for_dialect(D.MSSQLSRV),
    base.FuncDayofweek2(variants=[
        V(D.MSSQLSRV, lambda date, firstday: (
            ((sa.func.DATEDIFF(raw_sql('DAY'), EPOCH_START_S, date)
              - (EPOCH_START_DOW + base.norm_fd(firstday) - 1)) % 7 + 1))),
    ]),

    # genericnow
    base.FuncGenericNow(variants=[
        V(D.MSSQLSRV, lambda: sa.type_coerce(raw_sql('CURRENT_TIMESTAMP'), sa.DateTime)),
    ]),

    # hour
    base.FuncHourDate.for_dialect(D.MSSQLSRV),
    base.FuncHourDatetime(variants=[
        V(D.MSSQLSRV, lambda date: sa.func.DATEPART(raw_sql('hour'), date)),
    ]),

    # minute
    base.FuncMinuteDate.for_dialect(D.MSSQLSRV),
    base.FuncMinuteDatetime(variants=[
        V(D.MSSQLSRV, lambda date: sa.func.DATEPART(raw_sql('minute'), date)),
    ]),

    # month
    base.FuncMonth(variants=[
        V(D.MSSQLSRV, sa.func.MONTH),
    ]),

    # now
    base.FuncNow(variants=[
        V(D.MSSQLSRV, lambda: sa.type_coerce(raw_sql('CURRENT_TIMESTAMP'), sa.DateTime)),
    ]),

    # quarter
    base.FuncQuarter(variants=[
        V(D.MSSQLSRV, lambda date: sa.func.DATEPART(raw_sql('QUARTER'), date)),
    ]),

    # second
    base.FuncSecondDate.for_dialect(D.MSSQLSRV),
    base.FuncSecondDatetime(variants=[
        V(D.MSSQLSRV, lambda date: sa.func.DATEPART(raw_sql('second'), date)),
    ]),

    # today
    base.FuncToday(variants=[
        V(D.MSSQLSRV, lambda: sa.type_coerce(sa.cast(sa.func.GETDATE(), sa.Date()), sa.Date())),
    ]),

    # week
    base.FuncWeek(variants=[
        V(D.MSSQLSRV, lambda date: sa.func.DATEPART(raw_sql('ISO_WEEK'), date)),
    ]),

    # year
    base.FuncYear(variants=[
        V(D.MSSQLSRV, sa.func.YEAR),
    ]),
]
