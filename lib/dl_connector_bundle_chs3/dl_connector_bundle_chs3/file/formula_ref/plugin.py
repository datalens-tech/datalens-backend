from dl_formula_ref.plugins.base.plugin import FormulaRefPlugin

from dl_connector_bundle_chs3.file.formula.constants import FileS3Dialect
from dl_connector_bundle_chs3.file.formula_ref.human_dialects import HUMAN_DIALECTS
from dl_connector_bundle_chs3.file.formula_ref.i18n import CONFIGS


class FileS3FormulaRefPlugin(FormulaRefPlugin):
    any_dialects = frozenset((*FileS3Dialect.FILE.to_list(),))
    compeng_support_dialects = frozenset((*FileS3Dialect.FILE.to_list(),))
    human_dialects = HUMAN_DIALECTS
    translation_configs = frozenset(CONFIGS)
