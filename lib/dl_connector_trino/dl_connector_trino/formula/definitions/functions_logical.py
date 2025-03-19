import sqlalchemy as sa

from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.functions_logical as base

from dl_connector_trino.formula.constants import TrinoDialect as D


V = TranslationVariant.make


DEFINITIONS_LOGICAL = [
    # case
    base.FuncCase.for_dialect(D.TRINO),
    # if
    base.FuncIf.for_dialect(D.TRINO),
    # ifnan
    base.FuncIfnan(
        variants=[
            V(
                D.TRINO,
                lambda check_value, alt_value: sa.func.if_(sa.func.is_nan(check_value), alt_value, check_value),
            ),
        ]
    ),
    # ifnull
    base.FuncIfnull(
        variants=[
            V(D.TRINO, sa.func.coalesce),
        ]
    ),
    # iif
    base.FuncIif3Legacy.for_dialect(D.TRINO),
    # isnan
    base.FuncIsnan(
        variants=[
            V(D.TRINO, sa.func.is_nan),
        ]
    ),
    # isnull
    base.FuncIsnull.for_dialect(D.TRINO),
    # zn
    base.FuncZn(
        variants=[
            V(D.TRINO, lambda x: sa.func.coalesce(x, 0)),
        ]
    ),
]
