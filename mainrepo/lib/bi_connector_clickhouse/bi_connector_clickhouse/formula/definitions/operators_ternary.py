import bi_formula.definitions.operators_ternary as base

from bi_connector_clickhouse.formula.constants import ClickHouseDialect as D

DEFINITIONS_TERNARY = [
    # between
    base.TernaryBetween.for_dialect(D.CLICKHOUSE),
    # notbetween
    base.TernaryNotBetween.for_dialect(D.CLICKHOUSE),
]
