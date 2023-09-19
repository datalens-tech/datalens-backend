import sqlalchemy as sa

from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.functions_aggregation as base

from bi_connector_mysql.formula.constants import MySQLDialect as D


V = TranslationVariant.make


DEFINITIONS_AGG = [
    # any
    base.AggAny(
        variants=[
            V(D.and_above(D.MYSQL_5_7), sa.func.ANY_VALUE),
        ]
    ),
    # avg
    base.AggAvgFromNumber.for_dialect(D.MYSQL),
    base.AggAvgFromDate.for_dialect(D.MYSQL),
    base.AggAvgFromDatetime.for_dialect(D.MYSQL),
    base.AggAvgFromDatetimeTZ.for_dialect(D.MYSQL),
    # avg_if
    base.AggAvgIf.for_dialect(D.MYSQL),
    # count
    base.AggCount0.for_dialect(D.MYSQL),
    base.AggCount1.for_dialect(D.MYSQL),
    # count_if
    base.AggCountIf.for_dialect(D.MYSQL),
    # countd
    base.AggCountd.for_dialect(D.MYSQL),
    # countd_if
    base.AggCountdIf.for_dialect(D.MYSQL),
    # max
    base.AggMax.for_dialect(D.MYSQL),
    # min
    base.AggMin.for_dialect(D.MYSQL),
    # stdev
    base.AggStdev.for_dialect(D.MYSQL),
    # stdevp
    base.AggStdevp.for_dialect(D.MYSQL),
    # sum
    base.AggSum.for_dialect(D.MYSQL),
    # sum_if
    base.AggSumIf.for_dialect(D.MYSQL),
    # var
    base.AggVar.for_dialect(D.MYSQL),
    # varp
    base.AggVarp.for_dialect(D.MYSQL),
]
