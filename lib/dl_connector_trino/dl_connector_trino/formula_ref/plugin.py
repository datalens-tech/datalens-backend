from dl_formula_ref.plugins.base.plugin import FormulaRefPlugin

from dl_connector_trino.formula.constants import TrinoDialect
from dl_connector_trino.formula_ref.human_dialects import HUMAN_DIALECTS
from dl_connector_trino.formula_ref.i18n import CONFIGS


class TrinoFormulaRefPlugin(FormulaRefPlugin):
    any_dialects = frozenset((*TrinoDialect.TRINO.to_list(),))
    compeng_support_dialects = frozenset((*TrinoDialect.TRINO.to_list(),))
    human_dialects = HUMAN_DIALECTS
    translation_configs = frozenset(CONFIGS)
