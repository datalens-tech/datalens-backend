from dl_formula_ref.plugins.base.plugin import FormulaRefPlugin

from dl_connector_bundle_chs3.chs3_yadocs.formula.constants import YaDocsFileS3Dialect
from dl_connector_bundle_chs3.chs3_yadocs.formula_ref.human_dialects import HUMAN_DIALECTS
from dl_connector_bundle_chs3.chs3_yadocs.formula_ref.i18n import CONFIGS


class YaDocsFileS3FormulaRefPlugin(FormulaRefPlugin):
    any_dialects = frozenset((*YaDocsFileS3Dialect.YADOCS.to_list(),))
    compeng_support_dialects = frozenset((*YaDocsFileS3Dialect.YADOCS.to_list(),))
    human_dialects = HUMAN_DIALECTS
    translation_configs = frozenset(CONFIGS)
