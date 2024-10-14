from __future__ import annotations

from typing import TYPE_CHECKING

import sqlalchemy as sa
from sqlalchemy.sql.elements import ClauseElement

from dl_formula.definitions.base import (
    TranslationVariant,
    TranslationVariantWrapped,
)
from dl_formula.definitions.common import raw_sql
from dl_formula.definitions.common_datetime import (
    datetime_interval,
    ensure_naive_first_arg,
)
import dl_formula.definitions.functions_datetime as base
from dl_formula.definitions.literals import un_literal
from dl_formula.shortcuts import n

from dl_connector_postgresql.formula.constants import PostgreSQLDialect as D


if TYPE_CHECKING:
    from dl_formula.translation.context import TranslationCtx


V = TranslationVariant.make
VW = TranslationVariantWrapped.make


def _datetrunc2_pg_tz_impl(date_ctx: TranslationCtx, unit_ctx: TranslationCtx) -> ClauseElement:
    unit = base.norm_datetrunc_unit(unit_ctx.expression)
    assert date_ctx.data_type_params is not None
    timezone = date_ctx.data_type_params.timezone
    date_expr = date_ctx.expression

    # Convert to local time (because postgresql doesn't have timezone argument
    # to date_trunc). `timestamptz` -> `timestamp`.
    date_expr = sa.func.timezone(timezone, date_expr)
    date_expr = sa.func.date_trunc(unit, date_expr)
    # Convert back to an aware (UTC) datetime. `timestamp` -> `timestamptz`.
    # NOTE: this is ambiguous-interpretation that might lead to incorrect
    # results within shifts from DST.
    date_expr = sa.func.timezone(timezone, date_expr)

    # not sure why mypy thinks there might be None
    assert date_expr is not None
    return date_expr


DEFINITIONS_DATETIME = [
    # dateadd
    base.FuncDateadd1.for_dialect(D.POSTGRESQL),
    base.FuncDateadd2Unit(
        variants=[
            V(
                D.POSTGRESQL,
                lambda date, what: (
                    sa.cast(
                        date + datetime_interval(un_literal(what), 1, literal_type=True, literal_mult=True), sa.Date
                    )
                    if un_literal(what).lower() != "quarter"
                    else sa.cast(date + datetime_interval("month", 3, literal_type=True, literal_mult=True), sa.Date)
                ),
            ),
        ]
    ),
    base.FuncDateadd2Number.for_dialect(D.POSTGRESQL),
    base.FuncDateadd3Legacy.for_dialect(D.POSTGRESQL),
    base.FuncDateadd3DateConstNum(
        variants=[
            V(
                D.POSTGRESQL,
                lambda date, what, num: (
                    sa.cast(
                        (
                            date
                            + (
                                datetime_interval(
                                    un_literal(what), un_literal(num), literal_type=True, literal_mult=True
                                )
                                if un_literal(what).lower() != "quarter"
                                else datetime_interval(
                                    "month", 3 * un_literal(num), literal_type=True, literal_mult=True
                                )
                            )
                        ),
                        sa.Date,
                    )
                ),
            ),
        ]
    ),
    base.FuncDateadd3DatetimeConstNum(
        variants=[
            V(
                D.POSTGRESQL,
                lambda dt, what, num: (
                    dt
                    + (
                        datetime_interval(what.value, num.value, literal_type=True, literal_mult=True)
                        if what.value.lower() != "quarter"
                        else datetime_interval("month", 3 * num.value, literal_type=True, literal_mult=True)
                    )
                ),
            ),
        ]
    ),
    # datepart
    base.FuncDatepart2Legacy.for_dialect(D.POSTGRESQL),
    base.FuncDatepart2.for_dialect(D.POSTGRESQL),
    base.FuncDatepart3Const.for_dialect(D.POSTGRESQL),
    base.FuncDatepart3NonConst.for_dialect(D.POSTGRESQL),
    # datetrunc
    base.FuncDatetrunc2Date(
        variants=[
            V(
                D.POSTGRESQL,
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
            VW(
                D.POSTGRESQL,
                lambda date, unit: (sa.func.date_trunc(base.norm_datetrunc_unit(unit.expression), date.expression)),
            ),
        ]
    ),
    base.FuncDatetrunc2DatetimeTZ(
        variants=[
            VW(D.POSTGRESQL, _datetrunc2_pg_tz_impl),
        ]
    ),
    # day
    base.FuncDay(
        variants=[
            VW(D.POSTGRESQL, lambda date: sa.cast(sa.func.DATE_PART("day", date.expression), sa.BIGINT)),
        ]
    ),
    base.FuncDayDatetimeTZ(
        variants=[
            VW(D.POSTGRESQL, ensure_naive_first_arg(n.func.DAY)),
        ]
    ),
    # dayofweek
    base.FuncDayofweek1.for_dialect(D.POSTGRESQL),
    base.FuncDayofweek2(
        variants=[
            VW(
                D.POSTGRESQL,
                ensure_naive_first_arg(
                    lambda date_ctx, firstday_ctx: base.dow_firstday_shift(
                        # `date_part` returns a `double precision` value, so have to cast it;
                        # not casting to `SMALLINT` because pg doesn't expand typesize on e.g. multiplication.
                        sa.cast(sa.func.DATE_PART("isodow", date_ctx.expression), sa.BIGINT),
                        firstday_ctx.expression,
                    )
                ),
            ),
        ]
    ),
    base.FuncDayofweek2TZ(
        variants=[
            VW(D.POSTGRESQL, ensure_naive_first_arg(n.func.DAYOFWEEK)),
        ]
    ),
    # genericnow
    base.FuncGenericNow(
        variants=[
            V(D.POSTGRESQL, sa.func.NOW),
        ]
    ),
    # hour
    base.FuncHourDate.for_dialect(D.POSTGRESQL),
    base.FuncHourDatetime(
        variants=[
            VW(D.POSTGRESQL, lambda date: sa.cast(sa.func.DATE_PART("hour", date.expression), sa.BIGINT)),
        ]
    ),
    base.FuncHourDatetimeTZ(
        variants=[
            VW(D.POSTGRESQL, ensure_naive_first_arg(n.func.HOUR)),
        ]
    ),
    # minute
    base.FuncMinuteDate.for_dialect(D.POSTGRESQL),
    base.FuncMinuteDatetime(
        variants=[
            VW(D.POSTGRESQL, lambda date: sa.cast(sa.func.DATE_PART("minute", date.expression), sa.BIGINT)),
        ]
    ),
    base.FuncMinuteDatetimeTZ(
        variants=[
            VW(D.POSTGRESQL, ensure_naive_first_arg(n.func.MINUTE)),
        ]
    ),
    # month
    base.FuncMonth(
        variants=[
            VW(D.POSTGRESQL, lambda date: sa.cast(sa.func.DATE_PART("month", date.expression), sa.BIGINT)),
        ]
    ),
    base.FuncMonthDatetimeTZ(
        variants=[
            VW(D.POSTGRESQL, ensure_naive_first_arg(n.func.MONTH)),
        ]
    ),
    # now
    base.FuncNow(
        variants=[
            V(D.POSTGRESQL, sa.func.NOW),
        ]
    ),
    # quarter
    base.FuncQuarter(
        variants=[
            VW(D.POSTGRESQL, lambda date: sa.cast(sa.func.DATE_PART("quarter", date.expression), sa.BIGINT)),
        ]
    ),
    base.FuncQuarterDatetimeTZ(
        variants=[
            VW(D.POSTGRESQL, ensure_naive_first_arg(n.func.QUARTER)),
        ]
    ),
    # second
    base.FuncSecondDate.for_dialect(D.POSTGRESQL),
    base.FuncSecondDatetime(
        variants=[
            VW(D.POSTGRESQL, lambda date: sa.cast(sa.func.DATE_PART("second", date.expression), sa.BIGINT)),
        ]
    ),
    base.FuncSecondDatetimeTZ(
        variants=[
            VW(D.POSTGRESQL, ensure_naive_first_arg(n.func.SECOND)),
        ]
    ),
    # today
    base.FuncToday(
        variants=[
            V(D.POSTGRESQL, lambda: sa.type_coerce(raw_sql("CURRENT_DATE"), sa.Date)),
        ]
    ),
    # week
    base.FuncWeek(
        variants=[
            VW(D.POSTGRESQL, lambda date: sa.cast(sa.func.TO_CHAR(date.expression, "IW"), sa.BIGINT)),
        ]
    ),
    base.FuncWeekTZ(
        variants=[
            VW(D.POSTGRESQL, ensure_naive_first_arg(n.func.WEEK)),
        ]
    ),
    # year
    base.FuncYear(
        variants=[
            VW(D.POSTGRESQL, lambda date: sa.cast(sa.func.DATE_PART("year", date.expression), sa.BIGINT)),
        ]
    ),
    base.FuncYearDatetimeTZ(
        variants=[
            VW(D.POSTGRESQL, ensure_naive_first_arg(n.func.YEAR)),
        ]
    ),
]
