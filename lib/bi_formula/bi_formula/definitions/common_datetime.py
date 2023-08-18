from __future__ import annotations

import datetime
import functools
from typing import TYPE_CHECKING, Any, Tuple

import sqlalchemy as sa

from bi_formula.core import exc, nodes
from bi_formula.core.datatype import DataType
from bi_formula.definitions.literals import un_literal
from bi_formula.shortcuts import n

if TYPE_CHECKING:
    from sqlalchemy.sql.elements import ClauseElement


DAY_SEC = 3600 * 24
DAY_USEC = DAY_SEC * 1_000_000  # microseconds

EPOCH_START_S = '1970-01-01'
EPOCH_START_D = datetime.date(1970, 1, 1)
EPOCH_START_DOW = 4  # it was a Thursday

SUPPORTED_INTERVAL_TYPES = ('second', 'minute', 'hour', 'day', 'week', 'month', 'quarter', 'year')


def normalize_and_validate_datetime_interval_type(type_name: str) -> str:
    type_name = type_name.lower()
    if type_name not in SUPPORTED_INTERVAL_TYPES:
        raise exc.TranslationError('Invalid interval type: \'{}\''.format(type_name))
    return type_name


YQL_INTERVAL_FUNCS = {
    'second': 'IntervalFromSeconds', 'minute': 'IntervalFromMinutes',
    'hour': 'IntervalFromHours', 'day': 'IntervalFromDays',
}
YQL_SHIFT_FUNCS = {
    'month': 'ShiftMonths', 'quarter': 'ShiftQuarters', 'year': 'ShiftYears',
}


def _date_datetime_add_yql(value_expr, type_expr, mult_expr, *, const_mult: bool) -> ClauseElement:
    type_name = un_literal(type_expr)
    type_name = normalize_and_validate_datetime_interval_type(type_name)

    if not const_mult:
        # YQL requires a non-nullable mult;
        # so ensure it is such.
        mult_expr = sa.func.coalesce(mult_expr, 0)

    if type_name == 'week':
        type_name = 'day'
        mult_expr = mult_expr * 7

    func_name = YQL_INTERVAL_FUNCS.get(type_name)
    if func_name is not None:
        func = getattr(sa.func.DateTime, func_name)
        return value_expr + func(mult_expr)

    func_name = YQL_SHIFT_FUNCS.get(type_name)
    if func_name is not None:
        func = getattr(sa.func.DateTime, func_name)
        return func(value_expr, mult_expr)

    raise ValueError("Unexpectedly unsupported YQL datetime shift", type_name)


def date_add_yql(value_expr, type_expr, mult_expr, *, const_mult: bool) -> ClauseElement:
    expr = _date_datetime_add_yql(value_expr, type_expr, mult_expr, const_mult=const_mult)
    return sa.func.DateTime.MakeDate(expr)


def datetime_add_yql(value_expr, type_expr, mult_expr, *, const_mult: bool) -> ClauseElement:
    expr = _date_datetime_add_yql(value_expr, type_expr, mult_expr, const_mult=const_mult)
    return sa.func.DateTime.MakeDatetime(expr)


def datetime_interval_ch(type_name: str, mult: int) -> ClauseElement:
    type_name = normalize_and_validate_datetime_interval_type(type_name)
    func_name = 'toInterval{}'.format(type_name.capitalize())
    return getattr(sa.func, func_name)(mult)


def datetime_interval(
        type_name: str, mult: int, caps: bool = True,
        literal_mult: bool = False, literal_type: bool = False,
        ch_func: bool = False
) -> ClauseElement:
    if ch_func:
        return datetime_interval_ch(type_name, mult)

    type_name = normalize_and_validate_datetime_interval_type(type_name)

    if literal_mult:
        if literal_type:
            sql = 'INTERVAL \'{} {}\''.format(mult, type_name)
        else:
            sql = 'INTERVAL \'{}\' {}'.format(mult, type_name)
    else:
        sql = 'INTERVAL {} {}'.format(mult, type_name)

    if caps:
        sql = sql.upper()
    else:
        sql = sql.lower()

    return sa.text(sql)


def ensure_naive_datetime(datetime_ctx):
    """ Convert DATETIMETZ values to DATETIME values; leave the other values as-is """
    if isinstance(datetime_ctx, nodes.FuncCall) and datetime_ctx.name.lower() == 'datetimetz_to_naive':
        # double-ensure
        return datetime_ctx
    if datetime_ctx.data_type == DataType.DATETIMETZ:
        datetime_ctx = n.func.DATETIMETZ_TO_NAIVE(datetime_ctx)
    return datetime_ctx


def ensure_naive_first_arg(func):
    """ Make sure the first `func` argument is a naive DATETIME if it was a DATETIMETZ """
    # TODO: this should have probably been an autocast-by-function (same with str->markup)

    @functools.wraps(func)
    def _ensured_naive_first_arg(datetime_ctx, *args, **kwargs):
        datetime_ctx = ensure_naive_datetime(datetime_ctx)
        return func(datetime_ctx, *args, **kwargs)

    return _ensured_naive_first_arg


UTC_CH_TZ_ARGS = ('UTC',)  # TODO later: make empty (backwards-incompatible change)


def make_ch_tz_args(date_ctx) -> Tuple[Tuple[Any, ...], Tuple[Any, ...]]:
    """
    For a datetime TranslationCtx argument,
    return a pair of CH function `*args`:
      * for datetrunc-functions
      * for output `toDateTime` cast

    Intended for all cases, but especially for cases that return a datetime,
    such as `datetrunc`.
    """
    return (
        # processing-related TZ args
        (
            (date_ctx.data_type_params.timezone,)
            if date_ctx.data_type in (DataType.DATETIMETZ, DataType.CONST_DATETIMETZ)
            else ()
            if date_ctx.data_type in (DataType.GENERICDATETIME, DataType.CONST_GENERICDATETIME)
            else UTC_CH_TZ_ARGS
        ),
        # output-case TZ args
        (
            ()
            if date_ctx.data_type in (DataType.GENERICDATETIME, DataType.CONST_GENERICDATETIME)
            else ('UTC',)
        ),
    )


def ch_date_with_tz(date_ctx) -> Tuple[ClauseElement, ...]:
    """ Primarily intended for functions that return a non-datetime, such as toSecond() """
    date_expr = date_ctx.expression
    data_type = date_ctx.data_type
    if data_type in (DataType.NULL, DataType.DATE, DataType.CONST_DATE, DataType.GENERICDATETIME, DataType.CONST_GENERICDATETIME):
        return (date_expr,)

    tz_args, _ = make_ch_tz_args(date_ctx)
    if data_type == DataType.DATETIMETZ:
        return (date_expr,) + tz_args
    if data_type in (DataType.DATETIME, DataType.CONST_DATETIME):
        return (date_expr,) + tuple(sa.literal(arg) for arg in tz_args)

    raise ValueError("Unexpected data type in ch_date_with_tz", data_type)
