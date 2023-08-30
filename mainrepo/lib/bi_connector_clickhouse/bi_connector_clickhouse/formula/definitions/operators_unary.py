import sqlalchemy as sa

import bi_formula.definitions.operators_unary as base
from bi_formula.definitions.base import TranslationVariant

from bi_connector_clickhouse.formula.constants import ClickHouseDialect as D


V = TranslationVariant.make


DEFINITIONS_UNARY = [
    # isfalse
    base.UnaryIsFalseStringGeo.for_dialect(D.CLICKHOUSE),
    base.UnaryIsFalseNumbers.for_dialect(D.CLICKHOUSE),
    base.UnaryIsFalseDateTime(variants=[
        V(D.CLICKHOUSE, lambda x: sa.literal(0)),
    ]),
    base.UnaryIsFalseBoolean(variants=[
        V(D.CLICKHOUSE, lambda x: x == 0),
    ]),

    # istrue
    base.UnaryIsTrueStringGeo.for_dialect(D.CLICKHOUSE),
    base.UnaryIsTrueNumbers.for_dialect(D.CLICKHOUSE),
    base.UnaryIsTrueDateTime(variants=[
        V(D.CLICKHOUSE, lambda x: sa.literal(1)),
    ]),
    base.UnaryIsTrueBoolean(variants=[
        V(D.CLICKHOUSE, lambda x: x != 0),
    ]),

    # neg
    base.UnaryNegate.for_dialect(D.CLICKHOUSE),

    # not
    base.UnaryNotBool.for_dialect(D.CLICKHOUSE),
    base.UnaryNotNumbers.for_dialect(D.CLICKHOUSE),
    base.UnaryNotStringGeo.for_dialect(D.CLICKHOUSE),
    base.UnaryNotDateDatetime(variants=[
        V(D.CLICKHOUSE, lambda x: sa.literal(0)),
    ]),
]
