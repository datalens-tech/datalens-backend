from bi_formula_ref.plugins.base.plugin import FormulaRefPlugin

from bi_connector_mysql.formula.constants import MySQLDialect
from bi_connector_mysql.formula_ref.human_dialects import HUMAN_DIALECTS
from bi_connector_mysql.formula_ref.i18n import CONFIGS


class MySQLFormulaRefPlugin(FormulaRefPlugin):
    any_dialects = frozenset((
        *MySQLDialect.MYSQL.to_list(),
    ))
    compeng_support_dialects = frozenset((
        *MySQLDialect.MYSQL.to_list(),
    ))
    human_dialects = HUMAN_DIALECTS
    translation_configs = frozenset(CONFIGS)
