from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.operators_unary as base

from dl_connector_bigquery.formula.constants import BigQueryDialect as D

V = TranslationVariant.make


DEFINITIONS_UNARY = [
    # isfalse
    base.UnaryIsFalseStringGeo.for_dialect(D.BIGQUERY),
    base.UnaryIsFalseNumbers.for_dialect(D.BIGQUERY),
    base.UnaryIsFalseDateTime.for_dialect(D.BIGQUERY),
    base.UnaryIsFalseBoolean.for_dialect(D.BIGQUERY),
    # istrue
    base.UnaryIsTrueStringGeo.for_dialect(D.BIGQUERY),
    base.UnaryIsTrueNumbers.for_dialect(D.BIGQUERY),
    base.UnaryIsTrueDateTime.for_dialect(D.BIGQUERY),
    base.UnaryIsTrueBoolean.for_dialect(D.BIGQUERY),
    # neg
    base.UnaryNegate.for_dialect(D.BIGQUERY),
    # not
    base.UnaryNotBool.for_dialect(D.BIGQUERY),
    base.UnaryNotNumbers.for_dialect(D.BIGQUERY),
    base.UnaryNotStringGeo.for_dialect(D.BIGQUERY),
    base.UnaryNotDateDatetime.for_dialect(D.BIGQUERY),
]
