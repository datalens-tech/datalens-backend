from dl_formula_ref.plugins.base.plugin import FormulaRefPlugin

from dl_connector_bundle_chs3.chs3_gsheets.formula.constants import GSheetsFileS3Dialect
from dl_connector_bundle_chs3.chs3_gsheets.formula_ref.human_dialects import HUMAN_DIALECTS
from dl_connector_bundle_chs3.chs3_gsheets.formula_ref.i18n import CONFIGS


class GSheetsFileS3FormulaRefPlugin(FormulaRefPlugin):
    any_dialects = frozenset((*GSheetsFileS3Dialect.GSHEETS_V2.to_list(),))
    compeng_support_dialects = frozenset((*GSheetsFileS3Dialect.GSHEETS_V2.to_list(),))
    human_dialects = HUMAN_DIALECTS
    translation_configs = frozenset(CONFIGS)
