from __future__ import annotations

import bi_formula.definitions.functions_datetime as base

from bi_connector_gsheets.formula.constants import GSheetsDialect as D


DEFINITIONS_DATETIME = [
    # dateadd
    base.FuncDateadd1.for_dialect(D.GSHEETS),
    base.FuncDateadd2Unit.for_dialect(D.GSHEETS),
    base.FuncDateadd2Number.for_dialect(D.GSHEETS),
]
