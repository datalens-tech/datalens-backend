from bi_formula_ref.plugins.base.plugin import FormulaRefPlugin

from bi_connector_oracle.formula.constants import OracleDialect
from bi_connector_oracle.formula_ref.human_dialects import HUMAN_DIALECTS
from bi_connector_oracle.formula_ref.i18n import CONFIGS


class OracleFormulaRefPlugin(FormulaRefPlugin):
    any_dialects = frozenset((
        *OracleDialect.ORACLE.to_list(),
    ))
    compeng_support_dialects = frozenset((
        *OracleDialect.ORACLE.to_list(),
    ))
    human_dialects = HUMAN_DIALECTS
    translation_configs = frozenset(CONFIGS)
