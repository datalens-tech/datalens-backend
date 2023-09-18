import dl_formula.definitions.functions_type as base

from bi_connector_gsheets.formula.constants import GSheetsDialect as D


DEFINITIONS_TYPE = [
    # datetime
    base.FuncDatetime1FromDatetime.for_dialect(D.GSHEETS),

    # datetimetz
    base.FuncDatetimeTZConst.for_dialect(D.GSHEETS),

    # genericdatetime
    base.FuncGenericDatetime1FromDatetime.for_dialect(D.GSHEETS),
]
