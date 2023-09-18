import dl_formula.definitions.functions_string as base
from dl_formula.definitions.base import TranslationVariant

from bi_connector_metrica.formula.constants import MetricaDialect as D


V = TranslationVariant.make


DEFINITIONS_STRING = [
    # contains
    base.FuncContainsConst(variants=[
        V(D.METRIKAAPI, lambda x, y: x.contains(y)),
    ]),

    # endswith
    base.FuncEndswithConst(variants=[
        V(D.METRIKAAPI, lambda x, y: x.endswith(y)),
    ]),

    # startswith
    base.FuncStartswithConst(variants=[
        V(D.METRIKAAPI, lambda x, y: x.startswith(y)),
    ]),
]
