from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.operators_unary as base

from dl_connector_ydb.formula.constants import YqlDialect as D


V = TranslationVariant.make


DEFINITIONS_UNARY = [
    # isfalse
    base.UnaryIsFalseStringGeo.for_dialect(D.YQL),
    base.UnaryIsFalseNumbers.for_dialect(D.YQL),
    base.UnaryIsFalseDateTime.for_dialect(D.YQL),
    base.UnaryIsFalseBoolean(
        variants=[
            V(D.YQL, lambda x: x == False),  # noqa: E712
        ]
    ),
    # istrue
    base.UnaryIsTrueStringGeo.for_dialect(D.YQL),
    base.UnaryIsTrueNumbers.for_dialect(D.YQL),
    base.UnaryIsTrueDateTime.for_dialect(D.YQL),
    base.UnaryIsTrueBoolean(
        variants=[
            V(D.YQL, lambda x: x == True),  # noqa: E712
        ]
    ),
    # neg
    base.UnaryNegate.for_dialect(D.YQL),
    # not
    base.UnaryNotBool.for_dialect(D.YQL),
    base.UnaryNotNumbers.for_dialect(D.YQL),
    base.UnaryNotStringGeo.for_dialect(D.YQL),
    base.UnaryNotDateDatetime.for_dialect(D.YQL),
]
