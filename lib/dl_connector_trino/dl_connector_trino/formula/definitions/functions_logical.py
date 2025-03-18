from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.functions_logical as base

from dl_connector_trino.formula.constants import TrinoDialect as D


V = TranslationVariant.make


DEFINITIONS_LOGICAL = [
    # case
    # base.FuncCase.for_dialect(D.TRINO),
    # if
    # base.FuncIf.for_dialect(D.TRINO),
    # ifnull
    # base.FuncIfnull(
    #     variants=[
    #         V(D.TRINO, sa.func.COALESCE),
    #     ]
    # ),
    # iif
    # base.FuncIif3Legacy.for_dialect(D.TRINO),
    # isnull
    base.FuncIsnull(
        variants=[
            V(D.TRINO, lambda x: x.is_(None)),
        ]
    ),
    # zn
    # base.FuncZn(
    #     variants=[
    #         V(D.TRINO, lambda x: sa.func.COALESCE(x, 0)),
    #     ]
    # ),
]
