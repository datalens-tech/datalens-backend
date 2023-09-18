from typing import Type

from dl_formula_ref.config import register_config_version
from dl_formula_ref.i18n.registry import register_translation_configs
from dl_formula_ref.plugins.base.plugin import FormulaRefPlugin
from dl_formula_ref.texts import (
    register_any_dialects,
    register_human_dialects,
)


class FormulaRefPluginRegistrator:
    def register_plugin(self, plugin_cls: Type[FormulaRefPlugin]) -> None:
        for version, config in plugin_cls.configs.items():
            register_config_version(version=version, config=config)

        register_any_dialects(plugin_cls.any_dialects)
        register_human_dialects(plugin_cls.human_dialects)
        register_translation_configs(plugin_cls.translation_configs)


FORMULA_REF_PLUGIN_REG = FormulaRefPluginRegistrator()
