from dl_formula_ref.plugins.base.plugin import FormulaRefPlugin

from bi_connector_mssql.formula.constants import MssqlDialect
from bi_connector_mssql.formula_ref.human_dialects import HUMAN_DIALECTS
from bi_connector_mssql.formula_ref.i18n import CONFIGS


class MSSQLFormulaRefPlugin(FormulaRefPlugin):
    any_dialects = frozenset((
        *MssqlDialect.MSSQLSRV.to_list(),
    ))
    compeng_support_dialects = frozenset((
        *MssqlDialect.MSSQLSRV.to_list(),
    ))
    human_dialects = HUMAN_DIALECTS
    translation_configs = frozenset(CONFIGS)
