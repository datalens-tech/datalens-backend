from __future__ import annotations

import datetime
import functools
from typing import (
    TYPE_CHECKING,
    Any,
    Tuple,
)

import sqlalchemy as sa

from dl_formula.core import (
    exc,
    nodes,
)
from dl_formula.core.datatype import DataType
from dl_formula.shortcuts import n


if TYPE_CHECKING:
    from sqlalchemy.sql.elements import ClauseElement


DAY_SEC = 3600 * 24
DAY_USEC = DAY_SEC * 1_000_000  # microseconds

EPOCH_START_S = "1970-01-01"
EPOCH_START_D = datetime.date(1970, 1, 1)
EPOCH_START_DOW = 4  # it was a Thursday

SUPPORTED_INTERVAL_TYPES = ("second", "minute", "hour", "day", "week", "month", "quarter", "year")


def normalize_and_validate_datetime_interval_type(type_name: str) -> str:
    type_name = type_name.lower()
    if type_name not in SUPPORTED_INTERVAL_TYPES:
        raise exc.TranslationError("Invalid interval type: '{}'".format(type_name))
    return type_name


def datetime_interval(
    type_name: str,
    mult: int,
    caps: bool = True,
    literal_mult: bool = False,
    literal_type: bool = False,
) -> ClauseElement:
    type_name = normalize_and_validate_datetime_interval_type(type_name)

    if literal_mult:
        if literal_type:
            sql = "INTERVAL '{} {}'".format(mult, type_name)
        else:
            sql = "INTERVAL '{}' {}".format(mult, type_name)
    else:
        sql = "INTERVAL {} {}".format(mult, type_name)

    if caps:
        sql = sql.upper()
    else:
        sql = sql.lower()

    return sa.text(sql)


def ensure_naive_datetime(datetime_ctx):  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation  [no-untyped-def]
    """Convert DATETIMETZ values to DATETIME values; leave the other values as-is"""
    if isinstance(datetime_ctx, nodes.FuncCall) and datetime_ctx.name.lower() == "datetimetz_to_naive":
        # double-ensure
        return datetime_ctx
    if datetime_ctx.data_type == DataType.DATETIMETZ:
        datetime_ctx = n.func.DATETIMETZ_TO_NAIVE(datetime_ctx)
    return datetime_ctx


def ensure_naive_first_arg(func):  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation  [no-untyped-def]
    """Make sure the first `func` argument is a naive DATETIME if it was a DATETIMETZ"""
    # TODO: this should have probably been an autocast-by-function (same with str->markup)

    @functools.wraps(func)
    def _ensured_naive_first_arg(datetime_ctx, *args, **kwargs):  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation  [no-untyped-def]
        datetime_ctx = ensure_naive_datetime(datetime_ctx)
        return func(datetime_ctx, *args, **kwargs)

    return _ensured_naive_first_arg


UTC_CH_TZ_ARGS = ("UTC",)  # TODO later: make empty (backwards-incompatible change)


def make_ch_tz_args(date_ctx) -> Tuple[Tuple[Any, ...], Tuple[Any, ...]]:  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation for one or more arguments  [no-untyped-def]
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
            else (
                ()
                if date_ctx.data_type in (DataType.GENERICDATETIME, DataType.CONST_GENERICDATETIME)
                else UTC_CH_TZ_ARGS
            )
        ),
        # output-case TZ args
        (() if date_ctx.data_type in (DataType.GENERICDATETIME, DataType.CONST_GENERICDATETIME) else ("UTC",)),
    )


def ch_date_with_tz(date_ctx) -> Tuple[ClauseElement, ...]:  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation for one or more arguments  [no-untyped-def]
    """Primarily intended for functions that return a non-datetime, such as toSecond()"""
    date_expr = date_ctx.expression
    data_type = date_ctx.data_type
    if data_type in (
        DataType.NULL,
        DataType.DATE,
        DataType.CONST_DATE,
        DataType.GENERICDATETIME,
        DataType.CONST_GENERICDATETIME,
    ):
        return (date_expr,)

    tz_args, _ = make_ch_tz_args(date_ctx)
    if data_type == DataType.DATETIMETZ:
        return (date_expr,) + tz_args
    if data_type in (DataType.DATETIME, DataType.CONST_DATETIME):
        return (date_expr,) + tuple(sa.literal(arg) for arg in tz_args)

    raise ValueError("Unexpected data type in ch_date_with_tz", data_type)
