from dl_formula.core.dialect import (
    DialectName,
    DialectNamespace,
    simple_combo,
)

DIALECT_NAME_METRICAAPI = DialectName.declare("METRICAAPI")


class MetricaDialect(DialectNamespace):
    METRIKAAPI = simple_combo(name=DIALECT_NAME_METRICAAPI)
