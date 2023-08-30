from bi_formula_ref.config import ConfigVersion, DOC_GEN_CONFIG_DEFAULT
from bi_formula_ref.plugins.base.plugin import FormulaRefPlugin
from bi_formula_ref.plugins.default.i18n import CONFIGS


class DefaultFormulaRefPlugin(FormulaRefPlugin):
    configs = {
        ConfigVersion.default: DOC_GEN_CONFIG_DEFAULT,
    }
    translation_configs = frozenset(CONFIGS)
