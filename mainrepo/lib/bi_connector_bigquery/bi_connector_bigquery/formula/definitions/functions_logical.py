import sqlalchemy as sa

import bi_formula.definitions.functions_logical as base
from bi_formula.definitions.base import TranslationVariant

from bi_connector_bigquery.formula.constants import BigQueryDialect as D


V = TranslationVariant.make


DEFINITIONS_LOGICAL = [
    # case
    base.FuncCase.for_dialect(D.BIGQUERY),

    # if
    base.FuncIf.for_dialect(D.BIGQUERY),

    # ifnull
    base.FuncIfnull(variants=[
        V(D.BIGQUERY, sa.func.IFNULL),
    ]),

    # isnan
    base.FuncIsnan(variants=[
        V(D.BIGQUERY, sa.func.IS_NAN),
    ]),

    # isnull
    base.FuncIsnull.for_dialect(D.BIGQUERY),

    # zn
    base.FuncZn(variants=[
        V(D.BIGQUERY, lambda x: sa.func.IFNULL(x, 0)),
    ]),
]
