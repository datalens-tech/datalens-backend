from bi_formula_ref.plugins.base.plugin import FormulaRefPlugin

from bi_formula_ref_ya.config import DOC_GEN_CONFIG_YC
from bi_formula_ref_ya.constants import CONFIG_VERSION_YA


class YaFormulaRefPlugin(FormulaRefPlugin):
    configs = {
        CONFIG_VERSION_YA: DOC_GEN_CONFIG_YC,
    }
