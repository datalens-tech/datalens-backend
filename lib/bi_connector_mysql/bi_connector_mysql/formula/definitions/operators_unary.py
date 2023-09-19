import dl_formula.definitions.operators_unary as base

from bi_connector_mysql.formula.constants import MySQLDialect as D


DEFINITIONS_UNARY = [
    # isfalse
    base.UnaryIsFalseStringGeo.for_dialect(D.MYSQL),
    base.UnaryIsFalseNumbers.for_dialect(D.MYSQL),
    base.UnaryIsFalseDateTime.for_dialect(D.MYSQL),
    base.UnaryIsFalseBoolean.for_dialect(D.MYSQL),
    # istrue
    base.UnaryIsTrueStringGeo.for_dialect(D.MYSQL),
    base.UnaryIsTrueNumbers.for_dialect(D.MYSQL),
    base.UnaryIsTrueDateTime.for_dialect(D.MYSQL),
    base.UnaryIsTrueBoolean.for_dialect(D.MYSQL),
    # neg
    base.UnaryNegate.for_dialect(D.MYSQL),
    # not
    base.UnaryNotBool.for_dialect(D.MYSQL),
    base.UnaryNotNumbers.for_dialect(D.MYSQL),
    base.UnaryNotStringGeo.for_dialect(D.MYSQL),
    base.UnaryNotDateDatetime.for_dialect(D.MYSQL),
]
