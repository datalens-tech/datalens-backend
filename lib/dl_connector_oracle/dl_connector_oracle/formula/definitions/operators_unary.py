import sqlalchemy as sa

from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.operators_unary as base

from dl_connector_oracle.formula.constants import OracleDialect as D


V = TranslationVariant.make


DEFINITIONS_UNARY = [
    # isfalse
    base.UnaryIsFalseStringGeo(
        variants=[
            V(D.ORACLE, lambda x: x.is_(None)),
        ]
    ),
    base.UnaryIsFalseNumbers.for_dialect(D.ORACLE),
    base.UnaryIsFalseDateTime(
        variants=[
            V(D.ORACLE, lambda x: sa.literal(0)),
        ]
    ),
    base.UnaryIsFalseBoolean(
        variants=[
            V(D.ORACLE, lambda x: x == 0),
        ]
    ),
    # istrue
    base.UnaryIsTrueStringGeo(
        variants=[
            V(D.ORACLE, lambda x: x.isnot(None)),
        ]
    ),
    base.UnaryIsTrueNumbers.for_dialect(D.ORACLE),
    base.UnaryIsTrueDateTime(
        variants=[
            V(D.ORACLE, lambda x: sa.literal(1)),
        ]
    ),
    base.UnaryIsTrueBoolean(
        variants=[
            V(D.ORACLE, lambda x: x != 0),
        ]
    ),
    # neg
    base.UnaryNegate.for_dialect(D.ORACLE),
    # not
    base.UnaryNotBool.for_dialect(D.ORACLE),
    base.UnaryNotNumbers.for_dialect(D.ORACLE),
    base.UnaryNotStringGeo(
        variants=[
            V(D.ORACLE, lambda x: x.is_(None)),
        ]
    ),
    base.UnaryNotDateDatetime(
        variants=[
            V(D.ORACLE, lambda x: sa.literal(0)),
        ]
    ),
]
