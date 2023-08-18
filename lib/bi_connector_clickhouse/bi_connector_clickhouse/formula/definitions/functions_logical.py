import sqlalchemy as sa

import bi_formula.definitions.functions_logical as base
from bi_formula.definitions.base import (
    TranslationVariant, TranslationVariantWrapped,
)
from bi_formula.shortcuts import n

from bi_connector_clickhouse.formula.constants import ClickHouseDialect as D


V = TranslationVariant.make
VW = TranslationVariantWrapped.make


DEFINITIONS_LOGICAL = [
    # case
    base.FuncCase.for_dialect(D.CLICKHOUSE),

    # if
    base.FuncIf.for_dialect(D.CLICKHOUSE),

    # ifnan
    base.FuncIfnan(variants=[
        VW(
            D.CLICKHOUSE,
            lambda check_value, alt_value: n.func.IF(
                n.func.ISNAN(check_value),
                alt_value, check_value
            )
        ),
    ]),

    # ifnull
    base.FuncIfnull(variants=[
        V(D.CLICKHOUSE, sa.func.ifNull),
    ]),

    # iif
    base.FuncIif3Legacy.for_dialect(D.CLICKHOUSE),

    # isnan
    base.FuncIsnan(variants=[
        V(D.CLICKHOUSE, sa.func.isNaN),
    ]),

    # isnull
    base.FuncIsnull(variants=[
        V(D.CLICKHOUSE, sa.func.isNull),
    ]),

    # zn
    base.FuncZn(variants=[
        V(D.CLICKHOUSE, lambda x: sa.func.ifNull(x, 0)),
    ]),
]
