from dl_connector_postgresql.formula.constants import PostgreSQLDialect as D
import dl_formula.definitions.operators_unary as base


DEFINITIONS_UNARY = [
    # isfalse
    base.UnaryIsFalseStringGeo.for_dialect(D.POSTGRESQL),
    base.UnaryIsFalseNumbers.for_dialect(D.POSTGRESQL),
    base.UnaryIsFalseDateTime.for_dialect(D.POSTGRESQL),
    base.UnaryIsFalseBoolean.for_dialect(D.POSTGRESQL),
    # istrue
    base.UnaryIsTrueStringGeo.for_dialect(D.POSTGRESQL),
    base.UnaryIsTrueNumbers.for_dialect(D.POSTGRESQL),
    base.UnaryIsTrueDateTime.for_dialect(D.POSTGRESQL),
    base.UnaryIsTrueBoolean.for_dialect(D.POSTGRESQL),
    # neg
    base.UnaryNegate.for_dialect(D.POSTGRESQL),
    # not
    base.UnaryNotBool.for_dialect(D.POSTGRESQL),
    base.UnaryNotNumbers.for_dialect(D.POSTGRESQL),
    base.UnaryNotStringGeo.for_dialect(D.POSTGRESQL),
    base.UnaryNotDateDatetime.for_dialect(D.POSTGRESQL),
]
