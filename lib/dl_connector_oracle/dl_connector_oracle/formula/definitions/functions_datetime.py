from __future__ import annotations

from typing import TYPE_CHECKING

import sqlalchemy as sa

from dl_formula.core.datatype import DataType
from dl_formula.definitions.args import ArgTypeSequence
from dl_formula.definitions.base import TranslationVariant
from dl_formula.definitions.common import raw_sql
from dl_formula.definitions.common_datetime import (
    EPOCH_START_D,
    EPOCH_START_DOW,
    datetime_interval,
)
import dl_formula.definitions.functions_datetime as base
from dl_formula.definitions.literals import (
    literal,
    un_literal,
)

from dl_connector_oracle.formula.constants import OracleDialect as D


if TYPE_CHECKING:
    from sqlalchemy.sql.elements import ColumnElement


V = TranslationVariant.make


def _oracle_dateadd_impl(date_expr: ColumnElement, unit: str, num: int) -> ColumnElement:
    """
    Oracle implementation for dateadd that uses ADD_MONTHS for month-based intervals.
    This handles edge cases like adding a month to January 31st properly.
    """
    unit_lower = unit.lower()
    if unit_lower == "month":
        return sa.cast(sa.func.ADD_MONTHS(date_expr, num), sa.Date)
    elif unit_lower == "quarter":
        return sa.cast(sa.func.ADD_MONTHS(date_expr, 3 * num), sa.Date)
    elif unit_lower == "year":
        return sa.cast(sa.func.ADD_MONTHS(date_expr, 12 * num), sa.Date)
    return sa.cast(date_expr + datetime_interval(unit, num, literal_mult=True), sa.Date)


class FuncDatetrunc2Oracle(base.FuncDatetrunc2):
    _oracle_fmt_map = {
        "minute": "MI",
        "hour": "HH",
        "day": "DDD",
        "week": "IW",
        "month": "MM",
        "quarter": "Q",
        "year": "YYYY",
    }

    variants = [
        V(
            D.ORACLE,
            lambda date, unit: (
                sa.func.TRUNC(
                    date, literal(FuncDatetrunc2Oracle._oracle_fmt_map[base.norm_datetrunc_unit(unit)], d=D.ORACLE)
                )
                if base.norm_datetrunc_unit(unit) in FuncDatetrunc2Oracle._oracle_fmt_map
                else date
            ),
        ),
    ]
    argument_types = [
        ArgTypeSequence(
            [
                {DataType.DATE, DataType.DATETIME, DataType.GENERICDATETIME},  # TODO: DataType.DATETIMETZ
                DataType.CONST_STRING,
            ]
        ),
    ]


DEFINITIONS_DATETIME = [
    # dateadd
    base.FuncDateadd1.for_dialect(D.ORACLE),
    base.FuncDateadd2Unit(
        variants=[
            V(
                D.ORACLE,
                lambda date, what: _oracle_dateadd_impl(date, un_literal(what), 1),
            ),
        ]
    ),
    base.FuncDateadd2Number.for_dialect(D.ORACLE),
    base.FuncDateadd3Legacy.for_dialect(D.ORACLE),
    base.FuncDateadd3DateConstNum(
        variants=[
            V(
                D.ORACLE,
                lambda date, what, num: _oracle_dateadd_impl(date, un_literal(what), un_literal(num)),
            ),
        ]
    ),
    base.FuncDateadd3DatetimeConstNum(
        variants=[
            V(
                D.ORACLE,
                lambda dt, what, num: _oracle_dateadd_impl(dt, what.value, num.value),
            ),
        ]
    ),
    # datepart
    base.FuncDatepart2Legacy.for_dialect(D.ORACLE),
    base.FuncDatepart2.for_dialect(D.ORACLE),
    base.FuncDatepart3Const.for_dialect(D.ORACLE),
    base.FuncDatepart3NonConst.for_dialect(D.ORACLE),
    # datetrunc
    FuncDatetrunc2Oracle(),
    # day
    base.FuncDay(
        variants=[
            V(D.ORACLE, lambda date: sa.func.extract(raw_sql("DAY"), date)),
        ]
    ),
    # dayofweek
    base.FuncDayofweek1.for_dialect(D.ORACLE),
    base.FuncDayofweek2(
        variants=[
            V(
                D.ORACLE,
                lambda date, firstday: sa.cast(
                    date - EPOCH_START_D - (EPOCH_START_DOW + base.norm_fd(firstday) - 1), sa.Integer
                )
                % 7
                + 1,
            ),
        ]
    ),
    # genericnow
    base.FuncGenericNow(
        variants=[
            V(D.ORACLE, lambda: sa.type_coerce(sa.cast(raw_sql("SYSTIMESTAMP"), sa.Date), sa.DateTime)),
        ]
    ),
    # hour
    base.FuncHourDate.for_dialect(D.ORACLE),
    base.FuncHourDatetime(
        variants=[
            V(D.ORACLE, lambda date: sa.func.extract(raw_sql("HOUR"), sa.cast(date, sa.TIMESTAMP))),
        ]
    ),
    # minute
    base.FuncMinuteDate.for_dialect(D.ORACLE),
    base.FuncMinuteDatetime(
        variants=[
            V(D.ORACLE, lambda date: sa.func.extract(raw_sql("MINUTE"), sa.cast(date, sa.TIMESTAMP))),
        ]
    ),
    # month
    base.FuncMonth(
        variants=[
            V(D.ORACLE, lambda date: sa.func.extract(raw_sql("MONTH"), date)),
        ]
    ),
    # now
    base.FuncNow(
        variants=[
            V(D.ORACLE, lambda: sa.type_coerce(sa.cast(raw_sql("SYSTIMESTAMP"), sa.Date), sa.DateTime)),
        ]
    ),
    # quarter
    base.FuncQuarter(
        variants=[
            V(D.ORACLE, lambda date: sa.func.TRUNC((sa.func.extract(raw_sql("MONTH"), date) + 2) / 3)),
        ]
    ),
    # second
    base.FuncSecondDate.for_dialect(D.ORACLE),
    base.FuncSecondDatetime(
        variants=[
            V(D.ORACLE, lambda date: sa.func.extract(raw_sql("SECOND"), sa.cast(date, sa.TIMESTAMP))),
        ]
    ),
    # today
    base.FuncToday(
        variants=[
            V(D.ORACLE, lambda: sa.type_coerce(sa.func.TRUNC(sa.cast(raw_sql("SYSTIMESTAMP"), sa.Date)), sa.Date)),
        ]
    ),
    # week
    base.FuncWeek(
        variants=[
            V(D.ORACLE, lambda date: sa.cast(sa.func.TO_CHAR(date, "IW"), sa.Integer)),
        ]
    ),
    # year
    base.FuncYear(
        variants=[
            V(D.ORACLE, lambda date: sa.func.extract(raw_sql("YEAR"), date)),
        ]
    ),
]
