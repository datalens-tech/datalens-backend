from __future__ import annotations

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


V = TranslationVariant.make


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
                lambda date, what: (
                    sa.cast(date + datetime_interval(un_literal(what), 1, literal_mult=True), sa.Date)
                    if un_literal(what).lower() != "quarter"
                    else sa.cast(date + datetime_interval("month", 3, literal_mult=True), sa.Date)
                ),
            ),
        ]
    ),
    base.FuncDateadd2Number.for_dialect(D.ORACLE),
    base.FuncDateadd3Legacy.for_dialect(D.ORACLE),
    base.FuncDateadd3DateConstNum(
        variants=[
            V(
                D.ORACLE,
                lambda date, what, num: (
                    sa.cast(date + datetime_interval(un_literal(what), un_literal(num), literal_mult=True), sa.Date)
                    if un_literal(what).lower() != "quarter"
                    else sa.cast(date + datetime_interval("month", 3 * un_literal(num), literal_mult=True), sa.Date)
                ),
            ),
        ]
    ),
    base.FuncDateadd3DatetimeConstNum(
        variants=[
            V(
                D.ORACLE,
                lambda dt, what, num: (
                    sa.cast(dt + datetime_interval(what.value, num.value, literal_mult=True), sa.Date)
                    if what.value.lower() != "quarter"
                    else sa.cast(dt + datetime_interval("month", 3 * num.value, literal_mult=True), sa.Date)
                ),
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
