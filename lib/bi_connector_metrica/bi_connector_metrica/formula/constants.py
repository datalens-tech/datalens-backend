from bi_formula.core.dialect import DialectNamespace, DialectName, simple_combo


DIALECT_NAME_METRICAAPI = DialectName.declare('METRICAAPI')


class MetricaDialect(DialectNamespace):
    METRIKAAPI = simple_combo(name=DIALECT_NAME_METRICAAPI)
