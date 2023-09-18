import sqlalchemy as sa

import dl_formula.definitions.functions_logical as base
from dl_formula.definitions.base import (
    TranslationVariant,
)

from bi_connector_oracle.formula.constants import OracleDialect as D


V = TranslationVariant.make


DEFINITIONS_LOGICAL = [
    # case
    base.FuncCase.for_dialect(D.ORACLE),

    # if
    base.FuncIf.for_dialect(D.ORACLE),

    # ifnull
    base.FuncIfnull(variants=[
        V(D.ORACLE, sa.func.NVL),
    ]),

    # iif
    base.FuncIif3Legacy.for_dialect(D.ORACLE),

    # isnull
    base.FuncIsnull.for_dialect(D.ORACLE),

    # zn
    base.FuncZn(variants=[
        V(D.ORACLE, lambda x: sa.func.NVL(x, 0)),
    ]),
]
