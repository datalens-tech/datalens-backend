from dl_formula_ref.plugins.base.plugin import FormulaRefPlugin

from dl_connector_oracle.formula.constants import OracleDialect
from dl_connector_oracle.formula_ref.human_dialects import HUMAN_DIALECTS
from dl_connector_oracle.formula_ref.i18n import CONFIGS


class OracleFormulaRefPlugin(FormulaRefPlugin):
    any_dialects = frozenset((*OracleDialect.ORACLE.to_list(),))
    compeng_support_dialects = frozenset((*OracleDialect.ORACLE.to_list(),))
    human_dialects = HUMAN_DIALECTS
    translation_configs = frozenset(CONFIGS)
