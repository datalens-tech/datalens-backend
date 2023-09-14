from __future__ import annotations

from typing import TYPE_CHECKING

from bi_formula_ref.registry.arg_common import TypeStrategyInspector
from bi_formula_ref.registry.return_base import ReturnTypeExtractorBase

if TYPE_CHECKING:
    import bi_formula_ref.registry.base as _registry_base
    from bi_formula_ref.registry.env import GenerationEnvironment
    from bi_formula_ref.registry.text import ParameterizedText


class DefaultReturnTypeExtractor(ReturnTypeExtractorBase):
    def get_return_type(
        self,
        item: _registry_base.FunctionDocRegistryItem,
        env: GenerationEnvironment,
    ) -> ParameterizedText:
        ts_insp = TypeStrategyInspector()
        type_info = ts_insp.get_return_type_and_arg_type_note(item, env=env)
        return type_info.ret_type_str
