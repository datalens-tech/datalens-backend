from dl_formula_ref.config import (
    DOC_GEN_CONFIG_DEFAULT,
    ConfigVersion,
)
from dl_formula_ref.plugins.base.plugin import FormulaRefPlugin
from dl_formula_ref.plugins.default.i18n import CONFIGS


class DefaultFormulaRefPlugin(FormulaRefPlugin):
    configs = {
        ConfigVersion.default: DOC_GEN_CONFIG_DEFAULT,
    }
    translation_configs = frozenset(CONFIGS)
