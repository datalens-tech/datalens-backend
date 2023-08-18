from bi_formula.core.dialect import DialectNamespace, DialectName, simple_combo


class MetricaDialect(DialectNamespace):
    METRIKAAPI = simple_combo(name=DialectName.METRIKAAPI)
