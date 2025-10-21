from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.functions_native as base

from dl_connector_ydb.formula.constants import YqlDialect as D


V = TranslationVariant.make


DEFINITIONS_NATIVE = [
    base.DBCallInt.for_dialect(D.YQL),
    base.DBCallFloat.for_dialect(D.YQL),
    base.DBCallString.for_dialect(D.YQL),
    base.DBCallBool.for_dialect(D.YQL),
    base.DBCallArrayInt.for_dialect(D.YQL),
    base.DBCallArrayFloat.for_dialect(D.YQL),
    base.DBCallArrayString.for_dialect(D.YQL),
]
