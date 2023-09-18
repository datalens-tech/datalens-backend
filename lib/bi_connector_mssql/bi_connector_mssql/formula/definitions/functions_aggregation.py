import sqlalchemy as sa

import dl_formula.definitions.functions_aggregation as base
from dl_formula.definitions.base import (
    TranslationVariant,
)

from bi_connector_mssql.formula.constants import MssqlDialect as D


V = TranslationVariant.make


DEFINITIONS_AGG = [
    # avg
    base.AggAvgFromNumber.for_dialect(D.MSSQLSRV),
    base.AggAvgFromDate.for_dialect(D.MSSQLSRV),
    base.AggAvgFromDatetime.for_dialect(D.MSSQLSRV),
    base.AggAvgFromDatetimeTZ.for_dialect(D.MSSQLSRV),

    # avg_if
    base.AggAvgIf.for_dialect(D.MSSQLSRV),

    # count
    base.AggCount0.for_dialect(D.MSSQLSRV),
    base.AggCount1.for_dialect(D.MSSQLSRV),

    # count_if
    base.AggCountIf.for_dialect(D.MSSQLSRV),

    # countd
    base.AggCountd.for_dialect(D.MSSQLSRV),

    # countd_if
    base.AggCountdIf.for_dialect(D.MSSQLSRV),

    # max
    base.AggMax.for_dialect(D.MSSQLSRV),

    # min
    base.AggMin.for_dialect(D.MSSQLSRV),

    # stdev
    base.AggStdev(variants=[
        V(D.MSSQLSRV, sa.func.STDEV),
    ]),

    # stdevp
    base.AggStdevp(variants=[
        V(D.MSSQLSRV, sa.func.STDEVP),
    ]),

    # sum
    base.AggSum.for_dialect(D.MSSQLSRV),

    # sum_if
    base.AggSumIf.for_dialect(D.MSSQLSRV),

    # var
    base.AggVar(variants=[
        V(D.MSSQLSRV, sa.func.VAR),
    ]),

    # varp
    base.AggVarp(variants=[
        V(D.MSSQLSRV, sa.func.VARP),
    ]),
]
