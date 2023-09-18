import dl_formula.definitions.functions_type as base

from bi_connector_metrica.formula.constants import MetricaDialect as D

DEFINITIONS_TYPE = [
    # datetime
    base.FuncDatetime1FromDatetime.for_dialect(D.METRIKAAPI),
    # datetimetz
    base.FuncDatetimeTZConst.for_dialect(D.METRIKAAPI),
    # genericdatetime
    base.FuncGenericDatetime1FromDatetime.for_dialect(D.METRIKAAPI),
]
