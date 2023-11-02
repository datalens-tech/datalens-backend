from __future__ import annotations

from typing import Optional

import attr

from dl_formula_ref.i18n.registry import FormulaRefTranslatable
from dl_formula_ref.registry.aliased_res import (
    AliasedResourceRegistryBase,
    AliasedTextResource,
    SimpleAliasedResourceRegistry,
)
from dl_i18n.localizer_base import Translatable


@attr.s(frozen=True)
class ParameterizedText:
    text: Translatable = attr.ib(kw_only=True)
    resources: AliasedResourceRegistryBase = attr.ib(kw_only=True, factory=SimpleAliasedResourceRegistry)

    def __bool__(self) -> bool:
        return bool(self.text)

    @classmethod
    def from_str(cls, text: str, params: Optional[dict[str, str]] = None) -> ParameterizedText:
        resources = SimpleAliasedResourceRegistry(
            resources={key: AliasedTextResource(body=value) for key, value in (params or {}).items()},
        )
        return cls(text=FormulaRefTranslatable(s=text), resources=resources)
