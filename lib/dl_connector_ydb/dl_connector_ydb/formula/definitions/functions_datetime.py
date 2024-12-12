import sqlalchemy as sa
from sqlalchemy.sql.elements import ClauseElement

from dl_formula.connectors.base.literal import Literal
from dl_formula.definitions.base import (
    TranslationVariant,
    TranslationVariantWrapped,
)
from dl_formula.definitions.common_datetime import normalize_and_validate_datetime_interval_type
import dl_formula.definitions.functions_datetime as base
from dl_formula.definitions.literals import un_literal
from dl_formula.translation.context import TranslationCtx

from dl_connector_ydb.formula.constants import YqlDialect as D


V = TranslationVariant.make
VW = TranslationVariantWrapped.make


YQL_DATE_DATETRUNC_FUNCS = {
    "year": "StartOfYear",
    "quarter": "StartOfQuarter",
    "month": "StartOfMonth",
    "week": "StartOfWeek",
}


YQL_INTERVAL_FUNCS = {
    "second": "IntervalFromSeconds",
    "minute": "IntervalFromMinutes",
    "hour": "IntervalFromHours",
    "day": "IntervalFromDays",
}
YQL_SHIFT_FUNCS = {
    "month": "ShiftMonths",
    "quarter": "ShiftQuarters",
    "year": "ShiftYears",
}


def _date_datetime_add_yql(
    value_expr: ClauseElement, type_expr: Literal, mult_expr: ClauseElement, *, const_mult: bool
) -> ClauseElement:
    type_name = un_literal(type_expr)
    assert isinstance(type_name, str)
    type_name = normalize_and_validate_datetime_interval_type(type_name)

    if not const_mult:
        # YQL requires a non-nullable mult;
        # so ensure it is such.
        mult_expr = sa.func.coalesce(mult_expr, 0)

    if type_name == "week":
        type_name = "day"
        mult_expr = mult_expr * 7  # type: ignore  # 2024-04-02 # TODO: Unsupported operand types for * ("ClauseElement" and "int")  [operator]

    func_name = YQL_INTERVAL_FUNCS.get(type_name)
    if func_name is not None:
        func = getattr(sa.func.DateTime, func_name)
        return value_expr + func(mult_expr)

    func_name = YQL_SHIFT_FUNCS.get(type_name)
    if func_name is not None:
        func = getattr(sa.func.DateTime, func_name)
        return func(value_expr, mult_expr)

    raise ValueError("Unexpectedly unsupported YQL datetime shift", type_name)


def date_add_yql(
    value_expr: ClauseElement, type_expr: Literal, mult_expr: ClauseElement, *, const_mult: bool
) -> ClauseElement:
    expr = _date_datetime_add_yql(value_expr, type_expr, mult_expr, const_mult=const_mult)
    return sa.func.DateTime.MakeDate(expr)


def datetime_add_yql(
    value_expr: ClauseElement, type_expr: Literal, mult_expr: ClauseElement, *, const_mult: bool
) -> ClauseElement:
    expr = _date_datetime_add_yql(value_expr, type_expr, mult_expr, const_mult=const_mult)
    return sa.func.DateTime.MakeDatetime(expr)


def _datetrunc2_yql_impl(date_ctx: TranslationCtx, unit_ctx: TranslationCtx) -> ClauseElement:
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
                lambda date, unit: (
                    sa.func.DateTime.MakeDate(
                        getattr(sa.func.DateTime, YQL_DATE_DATETRUNC_FUNCS[base.norm_datetrunc_unit(unit)])(date)
                    )
                    if base.norm_datetrunc_unit(unit) in YQL_DATE_DATETRUNC_FUNCS
                    else date
                ),
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
