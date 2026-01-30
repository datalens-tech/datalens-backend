from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.operators_unary as base

from dl_connector_starrocks.formula.constants import StarRocksDialect as D


V = TranslationVariant.make


DEFINITIONS_UNARY = [
    # isfalse
    base.UnaryIsFalseStringGeo.for_dialect(D.STARROCKS),
    base.UnaryIsFalseNumbers.for_dialect(D.STARROCKS),
    base.UnaryIsFalseDateTime.for_dialect(D.STARROCKS),
    # StarRocks doesn't support IS FALSE syntax for booleans, use equality instead
    base.UnaryIsFalseBoolean(
        variants=[
            V(D.STARROCKS, lambda x: x == False),  # noqa: E712
        ]
    ),
    # istrue
    base.UnaryIsTrueStringGeo.for_dialect(D.STARROCKS),
    base.UnaryIsTrueNumbers.for_dialect(D.STARROCKS),
    base.UnaryIsTrueDateTime.for_dialect(D.STARROCKS),
    # StarRocks doesn't support IS TRUE syntax for booleans, use equality instead
    base.UnaryIsTrueBoolean(
        variants=[
            V(D.STARROCKS, lambda x: x == True),  # noqa: E712
        ]
    ),
    # neg
    base.UnaryNegate.for_dialect(D.STARROCKS),
    # not
    base.UnaryNotBool.for_dialect(D.STARROCKS),
    base.UnaryNotNumbers.for_dialect(D.STARROCKS),
    base.UnaryNotStringGeo.for_dialect(D.STARROCKS),
    base.UnaryNotDateDatetime.for_dialect(D.STARROCKS),
]
