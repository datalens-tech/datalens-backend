from typing import Type

from bi_formula_ref.plugins.base.plugin import FormulaRefPlugin
from bi_formula_ref.config import register_config_version


class FormulaRefPluginRegistrator:
    def register_plugin(self, plugin_cls: Type[FormulaRefPlugin]) -> None:
        for version, config in plugin_cls.configs.items():
            register_config_version(version=version, config=config)


FORMULA_REF_PLUGIN_REG = FormulaRefPluginRegistrator()
