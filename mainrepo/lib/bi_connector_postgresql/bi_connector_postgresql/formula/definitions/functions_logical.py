import sqlalchemy as sa

from bi_formula.definitions.base import (
    TranslationVariant,
    TranslationVariantWrapped,
)
import bi_formula.definitions.functions_logical as base
from bi_formula.shortcuts import n

from bi_connector_postgresql.formula.constants import PostgreSQLDialect as D

V = TranslationVariant.make
VW = TranslationVariantWrapped.make


DEFINITIONS_LOGICAL = [
    # case
    base.FuncCase.for_dialect(D.POSTGRESQL),
    # if
    base.FuncIf.for_dialect(D.POSTGRESQL),
    # ifnan
    base.FuncIfnan(
        variants=[
            VW(
                D.POSTGRESQL,
                lambda check_value, alt_value: n.func.IF(n.func.ISNAN(check_value), alt_value, check_value),
            ),
        ]
    ),
    # ifnull
    base.FuncIfnull(
        variants=[
            V(D.POSTGRESQL, sa.func.coalesce),
        ]
    ),
    # iif
    base.FuncIif3Legacy.for_dialect(D.POSTGRESQL),
    # isnan
    base.FuncIsnan(
        variants=[
            V(D.POSTGRESQL, lambda x: x == sa.text("double precision 'NaN'")),  # noqa
        ]
    ),
    # isnull
    base.FuncIsnull.for_dialect(D.POSTGRESQL),
    # zn
    base.FuncZn(
        variants=[
            V(D.POSTGRESQL, lambda x: sa.func.coalesce(x, 0)),
        ]
    ),
]
