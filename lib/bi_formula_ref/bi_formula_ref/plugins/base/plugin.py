from typing import ClassVar

from bi_formula_ref.config import ConfigVersion, RefDocGeneratorConfig


class FormulaRefPlugin:
    configs: ClassVar[dict[ConfigVersion, RefDocGeneratorConfig]] = {}
