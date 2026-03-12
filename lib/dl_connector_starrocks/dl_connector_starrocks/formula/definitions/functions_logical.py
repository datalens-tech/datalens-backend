import sqlalchemy as sa

from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.functions_logical as base

from dl_connector_starrocks.formula.constants import StarRocksDialect as D


V = TranslationVariant.make


DEFINITIONS_LOGICAL = [
    # case
    base.FuncCase.for_dialect(D.STARROCKS),
    # if
    base.FuncIf.for_dialect(D.STARROCKS),
    # ifnull
    base.FuncIfnull(
        variants=[
            V(D.STARROCKS, sa.func.IFNULL),
        ]
    ),
    # iif
    base.FuncIif3Legacy.for_dialect(D.STARROCKS),
    # isnull
    base.FuncIsnull(
        variants=[
            V(D.STARROCKS, sa.func.ISNULL),
        ]
    ),
    # zn
    base.FuncZn(
        variants=[
            V(D.STARROCKS, lambda x: sa.func.IFNULL(x, 0)),
        ]
    ),
]
