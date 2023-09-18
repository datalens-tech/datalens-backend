import dl_formula.definitions.functions_type as base

from bi_connector_bitrix_gds.formula.constants import BitrixDialect as D


DEFINITIONS_TYPE = [
    # date
    base.FuncDate1FromDatetime.for_dialect(D.BITRIX),
    base.FuncDate1FromString.for_dialect(D.BITRIX),

    # datetime
    base.FuncDatetime1FromDatetime.for_dialect(D.BITRIX),
    base.FuncDatetime1FromString.for_dialect(D.BITRIX),

    # datetimetz
    base.FuncDatetimeTZConst.for_dialect(D.BITRIX),

    # genericdatetime
    base.FuncGenericDatetime1FromDatetime.for_dialect(D.BITRIX),
    base.FuncGenericDatetime1FromString.for_dialect(D.BITRIX),
]
