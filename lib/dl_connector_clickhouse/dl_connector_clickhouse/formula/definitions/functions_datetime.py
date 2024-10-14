from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Union,
)

import clickhouse_sqlalchemy.types as ch_types
import sqlalchemy as sa
from sqlalchemy.sql.elements import ClauseElement

from dl_formula.definitions.base import (
    TranslationVariant,
    TranslationVariantWrapped,
)
from dl_formula.definitions.common_datetime import (
    ch_date_with_tz,
    datetime_interval,
    make_ch_tz_args,
    normalize_and_validate_datetime_interval_type,
)
import dl_formula.definitions.functions_datetime as base
from dl_formula.definitions.literals import un_literal

from dl_connector_clickhouse.formula.constants import ClickHouseDialect as D


if TYPE_CHECKING:
    from dl_formula.translation.context import TranslationCtx


V = TranslationVariant.make
VW = TranslationVariantWrapped.make


def _datetrunc2_ch_impl_19_3_3(
    date_ctx: TranslationCtx,
    unit_ctx: TranslationCtx,
) -> ClauseElement:
    return _datetrunc3_ch_impl_19_3_3(date_ctx, unit_ctx, num_ctx=1)


def _datetrunc3_ch_impl_19_3_3(
    date_ctx: TranslationCtx,
    unit_ctx: TranslationCtx,
    num_ctx: Union[TranslationCtx, int],
) -> ClauseElement:
    date_expr = date_ctx.expression
    unit = base.norm_datetrunc_unit(unit_ctx.expression)
    # for plugging the datetrunc2 here:
    num = num_ctx if isinstance(num_ctx, int) else un_literal(num_ctx.expression)  # type: ignore  # 2024-01-24 # TODO: Argument 1 to "un_literal" has incompatible type "ClauseElement | None"; expected "BaseLiteral | BindParameter[Any] | TypeDefiningCast | Function | Null | array | None"  [arg-type]
    tz_args, output_tz_args = make_ch_tz_args(date_ctx)
    if unit in {"year", "quarter", "month", "week"}:
        return sa.func.toDateTime(
            sa.func.toStartOfInterval(date_expr, datetime_interval(unit, num, caps=False), *tz_args), *output_tz_args
        )
    if unit in {"day", "hour", "minute", "second"}:
        return sa.func.toStartOfInterval(date_expr, datetime_interval(unit, num, caps=False), *tz_args)

    # This normally should not happen
    raise NotImplementedError(f"Unsupported unit {unit}")


def _datetime_interval_ch(type_name: str, mult: int) -> ClauseElement:
    type_name = normalize_and_validate_datetime_interval_type(type_name)
    func_name = "toInterval{}".format(type_name.capitalize())
    return getattr(sa.func, func_name)(mult)


DEFINITIONS_DATETIME = [
    # dateadd
    base.FuncDateadd1.for_dialect(D.CLICKHOUSE),
    base.FuncDateadd2Unit.for_dialect(D.CLICKHOUSE),
    base.FuncDateadd2Number.for_dialect(D.CLICKHOUSE),
    base.FuncDateadd3Legacy.for_dialect(D.CLICKHOUSE),
    base.FuncDateadd3DateNonConstNum(
        variants=[
            V(
                D.CLICKHOUSE,
                lambda date, what, num: sa.cast(
                    date + _datetime_interval_ch(what.value, sa.func.ifNull(num, 0)),
                    ch_types.Nullable(ch_types.Date),
                ),
            ),
        ]
    ),
    base.FuncDateadd3DatetimeNonConstNum(
        variants=[
            V(
                D.CLICKHOUSE,
                lambda dt, what, num: (
                    sa.func.toDateTime(dt + _datetime_interval_ch(what.value, sa.func.ifNull(num, 0)), "UTC")
                ),
            ),
        ]
    ),
    base.FuncDateadd3GenericDatetimeNonConstNum(
        variants=[
            V(
                D.CLICKHOUSE,
                lambda dt, what, num: dt + _datetime_interval_ch(what.value, sa.func.ifNull(num, 0)),
            ),
        ]
    ),
    base.FuncDateadd3DatetimeTZNonConstNum(
        variants=[
            V(
                D.CLICKHOUSE,
                lambda dt, what, num: dt + _datetime_interval_ch(what.value, sa.func.ifNull(num, 0)),
            ),
        ]
    ),
    # datepart
    base.FuncDatepart2Legacy.for_dialect(D.CLICKHOUSE),
    base.FuncDatepart2.for_dialect(D.CLICKHOUSE),
    base.FuncDatepart3Const.for_dialect(D.CLICKHOUSE),
    base.FuncDatepart3NonConst.for_dialect(D.CLICKHOUSE),
    # datetrunc
    base.FuncDatetrunc2Date(
        variants=[
            V(
                D.CLICKHOUSE,
                lambda date, unit: (
                    sa.func.toStartOfInterval(sa.func.toDate(date), datetime_interval(unit.value, 1, caps=False))
                    if base.norm_datetrunc_unit(unit) in {"year", "quarter", "month", "week"}
                    else sa.func.toDate(date)
                ),
            ),
        ]
    ),
    base.FuncDatetrunc2Datetime(
        variants=[
            VW(D.CLICKHOUSE, _datetrunc2_ch_impl_19_3_3),
        ]
    ),
    base.FuncDatetrunc2DatetimeTZ(
        variants=[
            VW(D.CLICKHOUSE, _datetrunc2_ch_impl_19_3_3),
        ]
    ),
    base.FuncDatetrunc3Date(
        variants=[
            V(
                D.CLICKHOUSE,
                lambda date, unit, num: (
                    sa.func.toStartOfInterval(
                        sa.func.toDate(date), datetime_interval(unit.value, num.value, caps=False)
                    )
                    if base.norm_datetrunc_unit(unit) in {"year", "quarter", "month", "week"}
                    else sa.func.toDate(
                        sa.func.toStartOfInterval(
                            sa.func.toDate(date), datetime_interval(unit.value, num.value, caps=False)
                        )
                    )
                    if base.norm_datetrunc_unit(unit) == "day"
                    else sa.func.toDate(date)
                ),
            ),
        ]
    ),
    base.FuncDatetrunc3Datetime(
        variants=[
            VW(D.CLICKHOUSE, _datetrunc3_ch_impl_19_3_3),
        ]
    ),
    # day
    base.FuncDay(
        variants=[
            VW(D.CLICKHOUSE, lambda date_ctx: sa.func.toDayOfMonth(*ch_date_with_tz(date_ctx))),
        ]
    ),
    base.FuncDayDatetimeTZ(
        variants=[
            VW(D.CLICKHOUSE, lambda date_ctx: sa.func.toDayOfMonth(*ch_date_with_tz(date_ctx))),
        ]
    ),
    # dayofweek
    base.FuncDayofweek1.for_dialect(D.CLICKHOUSE),
    base.FuncDayofweek2(
        variants=[
            VW(
                D.CLICKHOUSE,
                lambda date_ctx, firstday_ctx: base.dow_firstday_shift(
                    sa.func.toDayOfWeek(*ch_date_with_tz(date_ctx)), firstday_ctx.expression
                ),
            ),
        ]
    ),
    base.FuncDayofweek2TZ(
        variants=[
            VW(
                D.CLICKHOUSE,
                lambda date_ctx, firstday_ctx: base.dow_firstday_shift(
                    sa.func.toDayOfWeek(*ch_date_with_tz(date_ctx)), firstday_ctx.expression
                ),
            ),
        ]
    ),
    # genericnow
    base.FuncGenericNow(
        variants=[
            V(D.CLICKHOUSE, lambda: sa.func.toDateTime(sa.func.now())),
        ]
    ),
    # hour
    base.FuncHourDate.for_dialect(D.CLICKHOUSE),
    base.FuncHourDatetime(
        variants=[
            VW(D.CLICKHOUSE, lambda date_ctx: sa.func.toHour(*ch_date_with_tz(date_ctx))),
        ]
    ),
    base.FuncHourDatetimeTZ(
        variants=[
            VW(D.CLICKHOUSE, lambda date_ctx: sa.func.toHour(*ch_date_with_tz(date_ctx))),
        ]
    ),
    # minute
    base.FuncMinuteDate.for_dialect(D.CLICKHOUSE),
    base.FuncMinuteDatetime(
        variants=[
            VW(D.CLICKHOUSE, lambda date_ctx: sa.func.toMinute(*ch_date_with_tz(date_ctx))),
        ]
    ),
    base.FuncMinuteDatetimeTZ(
        variants=[
            VW(D.CLICKHOUSE, lambda date_ctx: sa.func.toMinute(*ch_date_with_tz(date_ctx))),
        ]
    ),
    # month
    base.FuncMonth(
        variants=[
            VW(D.CLICKHOUSE, lambda date_ctx: sa.func.toMonth(*ch_date_with_tz(date_ctx))),
        ]
    ),
    base.FuncMonthDatetimeTZ(
        variants=[
            VW(D.CLICKHOUSE, lambda date_ctx: sa.func.toMonth(*ch_date_with_tz(date_ctx))),
        ]
    ),
    # now
    base.FuncNow(
        variants=[
            V(D.CLICKHOUSE, lambda: sa.func.toDateTime(sa.func.now())),
        ]
    ),
    # quarter
    base.FuncQuarter(
        variants=[
            VW(D.CLICKHOUSE, lambda date_ctx: sa.func.toQuarter(*ch_date_with_tz(date_ctx))),
        ]
    ),
    base.FuncQuarterDatetimeTZ(
        variants=[
            VW(D.CLICKHOUSE, lambda date_ctx: sa.func.toQuarter(*ch_date_with_tz(date_ctx))),
        ]
    ),
    # second
    base.FuncSecondDate.for_dialect(D.CLICKHOUSE),
    base.FuncSecondDatetime(
        variants=[
            VW(D.CLICKHOUSE, lambda date_ctx: sa.func.toSecond(*ch_date_with_tz(date_ctx))),
        ]
    ),
    base.FuncSecondDatetimeTZ(
        variants=[
            VW(D.CLICKHOUSE, lambda date_ctx: sa.func.toSecond(*ch_date_with_tz(date_ctx))),
        ]
    ),
    # today
    base.FuncToday(
        variants=[
            V(D.CLICKHOUSE, lambda: sa.func.toDate(sa.func.now())),
        ]
    ),
    # week
    base.FuncWeek(
        variants=[
            VW(D.CLICKHOUSE, lambda date_ctx: sa.func.toISOWeek(*ch_date_with_tz(date_ctx))),
        ]
    ),
    base.FuncWeekTZ(
        variants=[
            VW(D.CLICKHOUSE, lambda date_ctx: sa.func.toISOWeek(*ch_date_with_tz(date_ctx))),
        ]
    ),
    # year
    base.FuncYear(
        variants=[
            VW(D.CLICKHOUSE, lambda date_ctx: sa.func.toYear(*ch_date_with_tz(date_ctx))),
        ]
    ),
    base.FuncYearDatetimeTZ(
        variants=[
            VW(D.CLICKHOUSE, lambda date_ctx: sa.func.toYear(*ch_date_with_tz(date_ctx))),
        ]
    ),
]
