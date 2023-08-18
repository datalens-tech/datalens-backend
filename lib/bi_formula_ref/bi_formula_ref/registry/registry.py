from __future__ import annotations

from typing import Any, Dict, Iterable, NamedTuple, Tuple

import attr

from bi_formula_ref.registry.env import GenerationEnvironment
from bi_formula_ref.registry.base import FunctionDocRegistryItem


class RefFunctionKey(NamedTuple):
    name: str
    category_name: str

    @classmethod
    def normalized(cls, name: str, category_name: str) -> RefFunctionKey:
        return RefFunctionKey(name=name.lower(), category_name=category_name.lower())


@attr.s
class FunctionReferenceRegistry:
    _registry: Dict[RefFunctionKey, FunctionDocRegistryItem] = attr.ib(kw_only=True, factory=dict)

    def add_item(self, item: FunctionDocRegistryItem) -> None:
        key = RefFunctionKey.normalized(name=item.name, category_name=item.category.name)
        self._registry[key] = item

    def items(self) -> Iterable[Tuple[RefFunctionKey, FunctionDocRegistryItem]]:
        return sorted(self._registry.items())

    def __contains__(self, key: Any) -> bool:
        if not isinstance(key, RefFunctionKey):
            return False
        return key in self._registry

    def __getitem__(self, key: Any) -> FunctionDocRegistryItem:
        return self._registry[key]

    def limit(self, env: GenerationEnvironment) -> FunctionReferenceRegistry:
        limited_reg = FunctionReferenceRegistry()
        for key, item in self.items():
            if item.is_supported(env=env):
                limited_reg.add_item(item)

        return limited_reg


FUNC_REFERENCE_REGISTRY = FunctionReferenceRegistry()
