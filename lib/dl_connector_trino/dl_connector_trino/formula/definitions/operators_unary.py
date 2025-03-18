from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.operators_unary as base

from dl_connector_trino.formula.constants import TrinoDialect as D


V = TranslationVariant.make

DEFINITIONS_UNARY = [
    # isfalse
    base.UnaryIsFalseStringGeo.for_dialect(D.TRINO),
    base.UnaryIsFalseNumbers.for_dialect(D.TRINO),
    base.UnaryIsFalseDateTime.for_dialect(D.TRINO),
    base.UnaryIsFalseBoolean(
        variants=[
            V(D.TRINO, lambda x: x == False),
        ]
    ),
    # istrue
    base.UnaryIsTrueStringGeo.for_dialect(D.TRINO),
    base.UnaryIsTrueNumbers(
        variants=[
            V(D.TRINO, lambda x: x != 0),
        ]
    ),
    base.UnaryIsTrueDateTime.for_dialect(D.TRINO),
    base.UnaryIsTrueBoolean(
        variants=[
            V(D.TRINO, lambda x: x == True),
        ]
    ),
    # neg
    base.UnaryNegate.for_dialect(D.TRINO),
    # not
    base.UnaryNotBool.for_dialect(D.TRINO),
    base.UnaryNotNumbers.for_dialect(D.TRINO),
    base.UnaryNotStringGeo.for_dialect(D.TRINO),
    base.UnaryNotDateDatetime.for_dialect(D.TRINO),
]
