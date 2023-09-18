import sqlalchemy as sa

import dl_formula.definitions.functions_logical as base
from dl_formula.definitions.base import (
    TranslationVariant,
)

from bi_connector_yql.formula.constants import YqlDialect as D


V = TranslationVariant.make


DEFINITIONS_LOGICAL = [
    # case
    base.FuncCase.for_dialect(D.YQL),

    # if
    base.FuncIf.for_dialect(D.YQL),

    # ifnull
    base.FuncIfnull(variants=[
        V(D.YQL, sa.func.coalesce),
    ]),

    # iif
    base.FuncIif3Legacy.for_dialect(D.YQL),

    # isnull
    base.FuncIsnull.for_dialect(D.YQL),

    # zn
    base.FuncZn(variants=[
        # See also: `NANVL()` to also replace `NaN`s.
        V(D.YQL, lambda x: sa.func.coalesce(x, 0)),
    ]),
]
