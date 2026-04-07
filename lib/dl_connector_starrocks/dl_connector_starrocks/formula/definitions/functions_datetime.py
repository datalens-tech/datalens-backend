import sqlalchemy as sa

from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.functions_datetime as base

from dl_connector_starrocks.formula.constants import StarRocksDialect as D


V = TranslationVariant.make


DEFINITIONS_DATETIME = [
    # TODO: BI-7171 dateadd, datepart, datetrunc
    # day
    base.FuncDay(
        variants=[
            V(D.STARROCKS, sa.func.DAYOFMONTH),
        ]
    ),
    # TODO: BI-7171 dayofweek
    # genericnow
    base.FuncGenericNow(
        variants=[
            V(D.STARROCKS, sa.func.NOW),
        ]
    ),
    # hour
    base.FuncHourDate.for_dialect(D.STARROCKS),
    base.FuncHourDatetime(
        variants=[
            V(D.STARROCKS, sa.func.HOUR),
        ]
    ),
    # minute
    base.FuncMinuteDate.for_dialect(D.STARROCKS),
    base.FuncMinuteDatetime(
        variants=[
            V(D.STARROCKS, sa.func.MINUTE),
        ]
    ),
    # month
    base.FuncMonth(
        variants=[
            V(D.STARROCKS, sa.func.MONTH),
        ]
    ),
    # now
    base.FuncNow(
        variants=[
            V(D.STARROCKS, sa.func.NOW),
        ]
    ),
    # quarter
    base.FuncQuarter(
        variants=[
            V(D.STARROCKS, sa.func.QUARTER),
        ]
    ),
    # second
    base.FuncSecondDate.for_dialect(D.STARROCKS),
    base.FuncSecondDatetime(
        variants=[
            V(D.STARROCKS, sa.func.SECOND),
        ]
    ),
    # today
    base.FuncToday(
        variants=[
            V(D.STARROCKS, sa.func.CURDATE),
        ]
    ),
    # week
    base.FuncWeek(
        variants=[
            V(D.STARROCKS, sa.func.WEEKOFYEAR),
        ]
    ),
    # year
    base.FuncYear(
        variants=[
            V(D.STARROCKS, sa.func.YEAR),
        ]
    ),
]
