from bi_formula_ref.plugins.base.plugin import FormulaRefPlugin

from bi_connector_yql.formula.constants import YqlDialect
from bi_connector_yql.formula_ref.human_dialects import HUMAN_DIALECTS


class YQLFormulaRefPlugin(FormulaRefPlugin):
    any_dialects = frozenset((
        *YqlDialect.YDB.to_list(),
    ))
    human_dialects = HUMAN_DIALECTS
