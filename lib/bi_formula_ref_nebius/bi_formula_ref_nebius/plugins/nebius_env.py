from bi_formula_ref.plugins.base.plugin import FormulaRefPlugin
from bi_formula_ref.config import DOC_GEN_CONFIG_DEFAULT

from bi_formula_ref_nebius.constants import CONFIG_VERSION_NEBIUS


class NebiusFormulaRefPlugin(FormulaRefPlugin):
    configs = {
        CONFIG_VERSION_NEBIUS: DOC_GEN_CONFIG_DEFAULT,
    }
