import sqlalchemy as sa

from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.operators_unary as base

from bi_connector_mssql.formula.constants import MssqlDialect as D


V = TranslationVariant.make


DEFINITIONS_UNARY = [
    # isfalse
    base.UnaryIsFalseStringGeo.for_dialect(D.MSSQLSRV),
    base.UnaryIsFalseNumbers.for_dialect(D.MSSQLSRV),
    base.UnaryIsFalseDateTime(
        variants=[
            V(D.MSSQLSRV, lambda x: sa.literal(0)),
        ]
    ),
    base.UnaryIsFalseBoolean(
        variants=[
            V(D.MSSQLSRV, lambda x: x == 0),
        ]
    ),
    # istrue
    base.UnaryIsTrueStringGeo.for_dialect(D.MSSQLSRV),
    base.UnaryIsTrueNumbers.for_dialect(D.MSSQLSRV),
    base.UnaryIsTrueDateTime(
        variants=[
            V(D.MSSQLSRV, lambda x: sa.literal(1)),
        ]
    ),
    base.UnaryIsTrueBoolean(
        variants=[
            V(D.MSSQLSRV, lambda x: x != 0),
        ]
    ),
    # neg
    base.UnaryNegate.for_dialect(D.MSSQLSRV),
    # not
    base.UnaryNotBool.for_dialect(D.MSSQLSRV),
    base.UnaryNotNumbers.for_dialect(D.MSSQLSRV),
    base.UnaryNotStringGeo.for_dialect(D.MSSQLSRV),
    base.UnaryNotDateDatetime(
        variants=[
            V(D.MSSQLSRV, lambda x: sa.literal(0)),
        ]
    ),
]
