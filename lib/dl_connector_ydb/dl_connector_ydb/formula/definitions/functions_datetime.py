from __future__ import annotations

import sqlalchemy as sa

from dl_formula.definitions.base import (
    TranslationVariant,
    TranslationVariantWrapped,
)
from dl_formula.definitions.common_datetime import (
    YQL_INTERVAL_FUNCS,
    date_add_yql,
    datetime_add_yql,
)
import dl_formula.definitions.functions_datetime as base

from dl_connector_ydb.formula.constants import YqlDialect as D


V = TranslationVariant.make
VW = TranslationVariantWrapped.make


YQL_DATE_DATETRUNC_FUNCS = {
    "year": "StartOfYear",
    "quarter": "StartOfQuarter",
    "month": "StartOfMonth",
    "week": "StartOfWeek",
}


def _datetrunc2_yql_impl(date_ctx, unit_ctx):
    date_expr = date_ctx.expression
    unit = base.norm_datetrunc_unit(unit_ctx.expression)

    func_name = YQL_DATE_DATETRUNC_FUNCS.get(unit)
    if func_name is not None:
        func = getattr(sa.func.DateTime, func_name)
        return sa.func.DateTime.MakeDatetime(func(date_expr))

    amount = 1
    func_name = YQL_INTERVAL_FUNCS.get(unit)
    if func_name is not None:
        func = getattr(sa.func.DateTime, func_name)
        return sa.func.DateTime.MakeDatetime(
            sa.func.DateTime.StartOf(
                date_expr,
                func(amount),
            )
        )

    # This normally should not happen
    raise NotImplementedError(f"Unsupported unit {unit}")


DEFINITIONS_DATETIME = [
    # dateadd
    base.FuncDateadd1.for_dialect(D.YQL),
    base.FuncDateadd2Unit.for_dialect(D.YQL),
    base.FuncDateadd2Number.for_dialect(D.YQL),
    base.FuncDateadd3DateConstNum(
        variants=[
            V(D.YQL, lambda date, what, num: date_add_yql(date, what, num, const_mult=True)),
        ]
    ),
    base.FuncDateadd3DateNonConstNum(
        variants=[
            V(D.YQL, lambda date, what, num: date_add_yql(date, what, num, const_mult=False)),
        ]
    ),
    base.FuncDateadd3DatetimeConstNum(
        variants=[
            V(D.YQL, lambda date, what, num: datetime_add_yql(date, what, num, const_mult=True)),
        ]
    ),
    base.FuncDateadd3DatetimeNonConstNum(
        variants=[
            V(D.YQL, lambda date, what, num: datetime_add_yql(date, what, num, const_mult=False)),
        ]
    ),
    base.FuncDateadd3GenericDatetimeNonConstNum(
        variants=[
            V(D.YQL, lambda date, what, num: datetime_add_yql(date, what, num, const_mult=False)),
        ]
    ),
    # datepart
    base.FuncDatepart2.for_dialect(D.YQL),
    base.FuncDatepart3Const.for_dialect(D.YQL),
    base.FuncDatepart3NonConst.for_dialect(D.YQL),
    # datetrunc
    base.FuncDatetrunc2Date(
        variants=[
            V(
                D.YQL,
                lambda date, unit: sa.func.DateTime.MakeDate(
                    getattr(sa.func.DateTime, YQL_DATE_DATETRUNC_FUNCS[base.norm_datetrunc_unit(unit)])(date)
                )
                if base.norm_datetrunc_unit(unit) in YQL_DATE_DATETRUNC_FUNCS
                else date,
            ),
        ]
    ),
    base.FuncDatetrunc2Datetime(
        variants=[
            VW(D.YQL, _datetrunc2_yql_impl),
        ]
    ),
    # day
    base.FuncDay(
        variants=[
            V(D.YQL, sa.func.DateTime.GetDayOfMonth),
        ]
    ),
    # dayofweek
    base.FuncDayofweek1.for_dialect(D.YQL),
    base.FuncDayofweek2(
        variants=[
            V(
                D.YQL,
                lambda date_expr, firstday_expr: base.dow_firstday_shift(
                    sa.func.DateTime.GetDayOfWeek(date_expr), firstday_expr
                ),
            ),
        ]
    ),
    # genericnow
    base.FuncGenericNow(
        variants=[
            V(D.YQL, sa.func.CurrentUtcDatetime),
        ]
    ),
    # hour
    base.FuncHourDate.for_dialect(D.YQL),
    base.FuncHourDatetime(
        variants=[
            V(D.YQL, sa.func.DateTime.GetHour),
        ]
    ),
    # minute
    base.FuncMinuteDate.for_dialect(D.YQL),
    base.FuncMinuteDatetime(
        variants=[
            V(D.YQL, sa.func.DateTime.GetMinute),
        ]
    ),
    # month
    base.FuncMonth(
        variants=[
            V(D.YQL, sa.func.DateTime.GetMonth),
        ]
    ),
    # now
    base.FuncNow(
        variants=[
            V(D.YQL, sa.func.CurrentUtcDatetime),
        ]
    ),
    # quarter
    base.FuncQuarter(
        variants=[
            V(D.YQL, lambda date: sa.cast((sa.func.DateTime.GetMonth(date) + 2) / 3, sa.INTEGER)),
        ]
    ),
    # second
    base.FuncSecondDate.for_dialect(D.YQL),
    base.FuncSecondDatetime(
        variants=[
            V(D.YQL, sa.func.DateTime.GetSecond),
        ]
    ),
    # today
    base.FuncToday(
        variants=[
            V(D.YQL, sa.func.CurrentUtcDate),  # https://ydb.tech/en/docs/yql/reference/syntax/not_yet_supported#now
        ]
    ),
    # week
    base.FuncWeek(
        variants=[
            V(D.YQL, sa.func.DateTime.GetWeekOfYearIso8601),
        ]
    ),
    # year
    base.FuncYear(
        variants=[
            V(D.YQL, sa.func.DateTime.GetYear),
        ]
    ),
]
