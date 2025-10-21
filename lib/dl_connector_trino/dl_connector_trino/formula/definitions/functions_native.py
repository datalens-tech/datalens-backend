from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.functions_native as base

from dl_connector_trino.formula.constants import TrinoDialect as D


V = TranslationVariant.make


DEFINITIONS_NATIVE = [
    base.DBCallInt.for_dialect(D.TRINO),
    base.DBCallFloat.for_dialect(D.TRINO),
    base.DBCallString.for_dialect(D.TRINO),
    base.DBCallBool.for_dialect(D.TRINO),
    base.DBCallArrayInt.for_dialect(D.TRINO),
    base.DBCallArrayFloat.for_dialect(D.TRINO),
    base.DBCallArrayString.for_dialect(D.TRINO),
]
