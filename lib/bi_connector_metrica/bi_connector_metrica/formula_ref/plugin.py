from dl_formula_ref.plugins.base.plugin import FormulaRefPlugin

from bi_connector_metrica.formula.constants import MetricaDialect
from bi_connector_metrica.formula_ref.human_dialects import HUMAN_DIALECTS


class MetricaFormulaRefPlugin(FormulaRefPlugin):
    any_dialects = frozenset((
        *MetricaDialect.METRIKAAPI.to_list(),
    ))
    human_dialects = HUMAN_DIALECTS
