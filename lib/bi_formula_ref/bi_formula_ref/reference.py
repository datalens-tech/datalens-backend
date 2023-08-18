from __future__ import annotations

from typing import Any, Dict, List, Collection, Type

from bi_formula.core.dialect import get_all_basic_dialects
from bi_formula.definitions.flags import ContextFlag
from bi_formula.definitions.base import MultiVariantTranslation

from bi_formula_ref.audience import Audience
from bi_formula_ref.primitives import RawFunc, RawMultiAudienceFunc
from bi_formula_ref.registry.base import FunctionDocRegistryItem
from bi_formula_ref.registry.env import GenerationEnvironment
from bi_formula_ref.registry.registry import RefFunctionKey, FUNC_REFERENCE_REGISTRY
from bi_formula_ref.registry.tools import populate_registry_from_definitions


class FuncReference:  # TODO: Merge with FunctionReferenceRegistry
    def __init__(self, funcs_by_key: Dict[RefFunctionKey, RawMultiAudienceFunc] = None):
        self._funcs_by_key = funcs_by_key or {}

    def __contains__(self, key: RefFunctionKey) -> bool:
        return key in self._funcs_by_key

    def add_func(self, func: RawMultiAudienceFunc) -> None:
        func_key = RefFunctionKey.normalized(name=func.name, category_name=func.category.name)
        self._funcs_by_key[func_key] = func

    def get_func(self, func_key: RefFunctionKey) -> RawMultiAudienceFunc:
        return self._funcs_by_key[func_key]

    def filter(self, category: str = None, name: str = None) -> List[RawMultiAudienceFunc]:
        result = []
        for func in self._funcs_by_key.values():
            if category is not None and func.category.name != category or name is not None and func.name != name:
                continue
            result.append(func)
        return result

    def as_list(self) -> List[RawMultiAudienceFunc]:
        return self.filter()


def _is_deprecated(defn: Type[MultiVariantTranslation]) -> bool:
    if not defn.return_flags:
        return False
    for dialect in get_all_basic_dialects():
        if defn._get_return_flags(dialect) & ContextFlag.DEPRECATED:
            return True
    return False


def _make_raw_func(item: FunctionDocRegistryItem, env: GenerationEnvironment) -> RawFunc:
    raw_func = RawFunc(
        name=item.name,
        title_factory=item.get_title,
        short_title_factory=item.get_short_title,
        internal_name=item.internal_name.upper(),
        description=item.description,
        dialects=item.get_dialects(env=env),
        args=item.get_args(env=env),
        signature_coll=item.get_signatures(env=env),
        notes=item.get_notes(env=env),
        return_type=item.get_return_type(env=env),
        category=item.category,
        resources=item.all_resources,
        examples=item.get_examples(env=env),
    )
    return raw_func


def _all_same(items: Collection[Any]) -> bool:
    if not items:
        return True
    one = next(iter(items))
    return all(item == one for item in items)


def load_func_reference_from_registry(scopes_by_audience: dict[Audience, int]) -> FuncReference:
    populate_registry_from_definitions()

    env_by_audience: dict[Audience, GenerationEnvironment] = {
        audience: GenerationEnvironment(scopes=aud_scopes)
        for audience, aud_scopes in scopes_by_audience.items()
    }

    func_ref = FuncReference()
    for key, item in FUNC_REFERENCE_REGISTRY.items():
        aud_func_dict: dict[Audience, RawFunc] = {}
        for audience, env in env_by_audience.items():
            if item.is_supported(env=env):
                aud_func_dict[audience] = _make_raw_func(item, env=env)
        if not aud_func_dict:
            continue

        all_raw_func_versions = list(aud_func_dict.values())
        if len(all_raw_func_versions) > 1 and _all_same(all_raw_func_versions):
            # Funcs for all audiences are the same, so consolidate them
            # under a single default audience
            aud_func_dict = {
                Audience(name='', default=True): all_raw_func_versions[0]
            }
        # Else keep the dict as-is
        multi_aud_func = RawMultiAudienceFunc.from_aud_dict(aud_func_dict)
        func_ref.add_func(multi_aud_func)

    return func_ref
