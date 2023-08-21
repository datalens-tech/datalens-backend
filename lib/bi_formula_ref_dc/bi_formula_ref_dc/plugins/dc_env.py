from bi_formula_ref.plugins.base.plugin import FormulaRefPlugin
from bi_formula_ref.config import DOC_GEN_CONFIG_DC

from bi_formula_ref_dc.constants import CONFIG_VERSION_DC


class DcFormulaRefPlugin(FormulaRefPlugin):
    configs = {
        CONFIG_VERSION_DC: DOC_GEN_CONFIG_DC,
    }
