from __future__ import annotations

import abc
from enum import Enum
from typing import (
    Callable,
    ClassVar,
    Iterable,
    Mapping,
)

import attr
import sqlalchemy as sa
from sqlalchemy.sql import ClauseElement

from dl_formula.core.datatype import DataType
from dl_formula.definitions.args import ArgTypeSequence
from dl_formula.definitions.base import TranslationVariant
from dl_formula.definitions.common_datetime import datetime_interval
import dl_formula.definitions.functions_datetime as base
from dl_formula.definitions.literals import un_literal

from dl_connector_mysql.formula.constants import MySQLDialect as D


V = TranslationVariant.make


class MySQLSubtractUnit(Enum):
    MONTH = "MONTH"
    DAY = "DAY"


class MySQLDatetruncStep(metaclass=abc.ABCMeta):
    """A step for building DATETRUNC clause for MySQL."""

    @abc.abstractmethod
    def step(self, date_clause: ClauseElement) -> ClauseElement:
        """Modifies original date clause according to the rules of given step."""
        raise NotImplementedError()


@attr.s(frozen=True)
class MySQLDatetruncSubtractUnitStep(MySQLDatetruncStep):
    """A step that evaluates unit value and then subtracts this unit value from date clause."""

    unit_to_subtract: MySQLSubtractUnit = attr.ib()
    unit_value_clause: Callable[[ClauseElement], ClauseElement] = attr.ib()

    def step(self, date_clause: ClauseElement) -> ClauseElement:
        subtract_units_expr = sa.func.TIMESTAMPADD(
            sa.text(self.unit_to_subtract.name),
            -self.unit_value_clause(date_clause).self_group(),
            date_clause,
        )
        return subtract_units_expr


@attr.s(frozen=True)
class MySQLDatetruncTruncateTimeStep(MySQLDatetruncStep):
    """A step that truncates time (sets it to 00:00:00) for date clause."""

    def step(self, date_clause: ClauseElement) -> ClauseElement:
        return sa.func.TIMESTAMP(sa.func.DATE(date_clause))


@attr.s(frozen=True)
class MySQLDatetruncRoundSecondsStep(MySQLDatetruncStep):
    """A step that rounds time to a multiple of round_quant seconds."""

    round_quant: int = attr.ib()

    def step(self, date_clause: ClauseElement) -> ClauseElement:
        time_sec = sa.func.time_to_sec(date_clause)
        rounded_time_sec = time_sec.op("div")(self.round_quant) * self.round_quant
        date = sa.func.date(date_clause)
        time = sa.func.sec_to_time(rounded_time_sec)
        return sa.func.timestamp(sa.func.concat(date, " ", time))


@attr.s(frozen=True)
class MySQLDatetruncCompositeStep(MySQLDatetruncStep):
    """A step which combines other steps and runs them in sequence."""

    steps: Iterable[MySQLDatetruncStep] = attr.ib()

    def step(self, date_clause: ClauseElement) -> ClauseElement:
        cur_clause = date_clause
        for step in self.steps:
            cur_clause = step.step(cur_clause)
        return cur_clause


class MySQLDatetruncBuildMixin:
    DATE_BUILD_DATA: ClassVar[Mapping[str, MySQLDatetruncStep]] = {}

    @classmethod
    def make_datetrunc(cls, date, unit):  # type: ignore
        norm_unit = base.norm_datetrunc_unit(unit)
        if norm_unit not in cls.DATE_BUILD_DATA:
            return date

        build_data: MySQLDatetruncStep = cls.DATE_BUILD_DATA[norm_unit]
        return build_data.step(date)


def round_month_value_clause_factory(round_quant: int) -> Callable[[ClauseElement], ClauseElement]:
    def round_month_value_clause(date_clause: ClauseElement) -> ClauseElement:
        # MONTH is 1-based thus an offset of -1 to compensate for that
        zero_based_value = sa.func.MONTH(date_clause) - sa.text("1")
        if round_quant == 1:
            return zero_based_value
        return zero_based_value % round_quant

    return round_month_value_clause


set_day_to_1 = MySQLDatetruncSubtractUnitStep(MySQLSubtractUnit.DAY, lambda date_clause: sa.func.DAY(date_clause) - 1)
set_month_to_1 = MySQLDatetruncSubtractUnitStep(MySQLSubtractUnit.MONTH, round_month_value_clause_factory(1))
trunc_month_count_to_quarter = MySQLDatetruncSubtractUnitStep(
    MySQLSubtractUnit.MONTH, round_month_value_clause_factory(3)  # A quarter = 3 months
)
# WEEKDAY(<Monday>) = 0, so correct for Monday-based week
trunc_week = MySQLDatetruncSubtractUnitStep(MySQLSubtractUnit.DAY, lambda date_clause: sa.func.WEEKDAY(date_clause))
trunc_month = set_day_to_1
trunc_quarter = MySQLDatetruncCompositeStep([trunc_month_count_to_quarter, set_day_to_1])
trunc_year = MySQLDatetruncCompositeStep([set_month_to_1, set_day_to_1])
set_time_to_0 = MySQLDatetruncTruncateTimeStep()


class FuncDatetrunc2DateMySQL(MySQLDatetruncBuildMixin, base.FuncDatetrunc2Date):
    DATE_BUILD_DATA: ClassVar[Mapping[str, MySQLDatetruncStep]] = {
        "year": trunc_year,
        "quarter": trunc_quarter,
        "month": trunc_month,
        "week": trunc_week,
    }

    variants = [
        V(D.MYSQL, lambda date, unit: FuncDatetrunc2DateMySQL.make_datetrunc(date, unit)),
    ]


class FuncDatetrunc2DatetimeMySQL(MySQLDatetruncBuildMixin, base.FuncDatetrunc2Datetime):
    DATE_BUILD_DATA: ClassVar[Mapping[str, MySQLDatetruncStep]] = {
        "year": MySQLDatetruncCompositeStep([trunc_year, set_time_to_0]),
        "quarter": MySQLDatetruncCompositeStep([trunc_quarter, set_time_to_0]),
        "month": MySQLDatetruncCompositeStep([trunc_month, set_time_to_0]),
        "week": MySQLDatetruncCompositeStep([trunc_week, set_time_to_0]),
        "day": set_time_to_0,
        "hour": MySQLDatetruncRoundSecondsStep(3600),
        "minute": MySQLDatetruncRoundSecondsStep(60),
        "second": MySQLDatetruncRoundSecondsStep(1),
    }

    variants = [
        V(D.MYSQL, lambda date, unit: FuncDatetrunc2DatetimeMySQL.make_datetrunc(date, unit)),
    ]
    argument_types = [
        ArgTypeSequence([{DataType.DATETIME, DataType.GENERICDATETIME}, DataType.CONST_STRING]),
    ]


DEFINITIONS_DATETIME = [
    # dateadd
    base.FuncDateadd1.for_dialect(D.MYSQL),
    base.FuncDateadd2Unit.for_dialect(D.MYSQL),
    base.FuncDateadd2Number.for_dialect(D.MYSQL),
    base.FuncDateadd3Legacy.for_dialect(D.MYSQL),
    base.FuncDateadd3DateConstNum(
        variants=[
            V(
                D.MYSQL,
                lambda date, what, num: sa.type_coerce(
                    date + datetime_interval(un_literal(what), un_literal(num)), sa.Date
                ),
            ),
        ]
    ),
    base.FuncDateadd3DatetimeConstNum(
        variants=[
            V(D.MYSQL, lambda dt, what, num: (dt + datetime_interval(what.value, num.value))),
        ]
    ),
    # datepart
    base.FuncDatepart2Legacy.for_dialect(D.MYSQL),
    base.FuncDatepart2.for_dialect(D.MYSQL),
    base.FuncDatepart3Const.for_dialect(D.MYSQL),
    base.FuncDatepart3NonConst.for_dialect(D.MYSQL),
    # datetrunc
    FuncDatetrunc2DateMySQL(),
    FuncDatetrunc2DatetimeMySQL(),
    # day
    base.FuncDay(
        variants=[
            V(D.MYSQL, sa.func.DAYOFMONTH),
        ]
    ),
    # dayofweek
    base.FuncDayofweek1.for_dialect(D.MYSQL),
    base.FuncDayofweek2(
        variants=[
            V(
                D.MYSQL, lambda date, firstday: base.dow_firstday_shift(sa.func.WEEKDAY(date) + 1, firstday)
            ),  # mysql weekday is 0 for monday, 1 for tuesday, ..., 6 for sunday.
        ]
    ),
    # genericnow
    base.FuncGenericNow(
        variants=[
            V(D.MYSQL, sa.func.NOW),
        ]
    ),
    # hour
    base.FuncHourDate.for_dialect(D.MYSQL),
    base.FuncHourDatetime(
        variants=[
            V(D.MYSQL, sa.func.HOUR),
        ]
    ),
    # minute
    base.FuncMinuteDate.for_dialect(D.MYSQL),
    base.FuncMinuteDatetime(
        variants=[
            V(D.MYSQL, sa.func.MINUTE),
        ]
    ),
    # month
    base.FuncMonth(
        variants=[
            V(D.MYSQL, sa.func.MONTH),
        ]
    ),
    # now
    base.FuncNow(
        variants=[
            V(D.MYSQL, sa.func.NOW),
        ]
    ),
    # quarter
    base.FuncQuarter(
        variants=[
            V(D.MYSQL, sa.func.QUARTER),
        ]
    ),
    # second
    base.FuncSecondDate.for_dialect(D.MYSQL),
    base.FuncSecondDatetime(
        variants=[
            V(D.MYSQL, sa.func.SECOND),
        ]
    ),
    # today
    base.FuncToday(
        variants=[
            V(D.MYSQL, sa.func.CURDATE),
        ]
    ),
    # week
    base.FuncWeek(
        variants=[
            V(D.MYSQL, sa.func.WEEKOFYEAR),
        ]
    ),
    # year
    base.FuncYear(
        variants=[
            V(D.MYSQL, sa.func.YEAR),
        ]
    ),
]
